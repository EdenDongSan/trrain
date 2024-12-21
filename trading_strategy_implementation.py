import logging
from typing import Optional, Tuple
from dataclasses import dataclass
import asyncio
import time
from order_execution import OrderExecutor
from market_data_manager import MarketDataManager
from models import Position
import math

logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    leverage: int = 30
    stop_loss_pct: float = 0.3
    take_profit_pct: float = 1.5
    volume_threshold: float = 20.0
    stoch_rsi_high: float = 90.0
    stoch_rsi_low: float = 10.0
    position_size_pct: float = 95.0  # 계좌잔고의 사용 비율

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.in_position = False
        self.last_volume = 0.0
        self.last_trade_time = 0
        self.min_trade_interval = 120
        
        # 포지션 비율 관련 변수 초기화
        self.entry_position_ratio = 0.0  # 진입 시점의 포지션 비율
        self.ratio_drop_threshold = 0.2  # 비율 하락 감지 임계값
        self.ratio_drop_value = 0.0  # 비율 하락으로 인한 청산 시점의 비율
        self.ratio_drop_direction = None  # 비율 하락이 발생한 포지션 방향

    async def calculate_position_size(self, current_price: float) -> float:
        """계좌 잔고를 기반으로 포지션 크기 계산"""
        try:
            account_info = await self.order_executor.api.get_account_balance()
            logger.info(f"Account info received: {account_info}")
            
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
            
            account_data = account_info.get('data', [])[0]
            available_balance = float(account_data.get('available', '0'))
            logger.info(f"Available balance: {available_balance}")
            
            trade_amount = available_balance * (self.config.position_size_pct / 100)
            position_size = (trade_amount * self.config.leverage) / current_price
            floor_size = math.floor(position_size * 1000) / 1000
            
            logger.info(f"Original size: {position_size}, Floor size: {floor_size}")
            return floor_size
                
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0

    def check_ratio_recovery(self, indicators: dict) -> bool:
        """비율 하락으로 인한 청산 후 회복 여부 확인"""
        try:
            if not self.ratio_drop_value or not self.ratio_drop_direction:
                return True
                
            # 현재 비율 가져오기
            if self.ratio_drop_direction == 'long':
                current_ratio = indicators.get('long_ratio', 0)
            else:
                current_ratio = indicators.get('short_ratio', 0)
                
            # 비율이 하락 시점 대비 0.1% 이상 회복되었는지 확인
            recovered = current_ratio > (self.ratio_drop_value + 0.1)
            
            if recovered:
                # 회복 확인되면 관련 변수들 초기화
                self.ratio_drop_value = 0.0
                self.ratio_drop_direction = None
                logger.info(f"포지션 비율 회복 확인: {current_ratio}%")
                
            return recovered
            
        except Exception as e:
            logger.error(f"비율 회복 확인 중 에러: {e}")
            return True  # 에러 발생 시 안전하게 True 반환

    def should_open_long(self, indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            # 비율 하락으로 인한 청산 후 회복 여부 체크
            if self.ratio_drop_direction == 'long' and not self.check_ratio_recovery(indicators):
                logger.info("롱 포지션 비율 미회복으로 진입 제한")
                return False

            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) < float(self.config.stoch_rsi_low)
            price_above_ema = float(indicators['last_close']) > float(indicators['ema200'])
            price_rising = float(indicators['price_change']) > 0
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_above_ema and 
                price_rising and 
                not self.in_position
            )
                
            logger.info(f"Long Entry Conditions:\n"
                     f"  Volume ({indicators['last_volume']:.2f} > {self.config.volume_threshold}): {volume_surge}\n"
                     f"  Stoch RSI K ({indicators['stoch_k']:.2f} < {self.config.stoch_rsi_low}): {stoch_rsi_condition}\n"
                     f"  Price Above EMA200 ({indicators['last_close']:.2f} > {indicators['ema200']:.2f}): {price_above_ema}\n"
                     f"  Price Rising ({indicators['price_change']:.2f} > 0): {price_rising}\n"
                     f"  No Position: {not self.in_position}\n"
                     f"  Ratio Lock: {self.ratio_drop_direction != 'long'}")
                
            return should_enter
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in should_open_long: {e}")
            return False

    def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        logger.info("숏 포지션 진입 조건 확인 시작")
        try:
            # 비율 하락으로 인한 청산 후 회복 여부 체크
            if self.ratio_drop_direction == 'short' and not self.check_ratio_recovery(indicators):
                logger.info("숏 포지션 비율 미회복으로 진입 제한")
                return False

            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_high)
            price_below_ema = float(indicators['last_close']) < float(indicators['ema200'])
            price_falling = float(indicators['price_change']) < 0

            logger.info(f"Short Entry Conditions:\n"
                    f"  Volume ({indicators['last_volume']:.2f} > {self.config.volume_threshold}): {volume_surge}\n"
                    f"  Stoch RSI K ({indicators['stoch_k']:.2f} > {self.config.stoch_rsi_high}): {stoch_rsi_condition}\n"
                    f"  Price Below EMA200 ({indicators['last_close']:.2f} < {indicators['ema200']:.2f}): {price_below_ema}\n"
                    f"  Price Falling ({indicators['price_change']:.2f} < 0): {price_falling}\n"
                    f"  No Position: {not self.in_position}\n"
                    f"  Ratio Lock: {self.ratio_drop_direction != 'short'}")
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_below_ema and 
                price_falling and 
                not self.in_position
            )
            
            return should_enter
                    
        except KeyError as e:
            logger.error(f"Missing indicator in should_open_short: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in should_open_short: {e}")
            return False

    async def execute_long_trade(self, current_price: float):
        """롱 포지션 Limit 진입 실행"""
        try:
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")

            # 현재 롱 비율 저장
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            self.entry_position_ratio = float(position_ratios.get('long_ratio', 0))
            logger.info(f"저장된 진입 시점 롱 비율: {self.entry_position_ratio}%")
            
            size = await self.calculate_position_size(current_price) 
            if size == 0:
                return
            
            entry_price = current_price
            stop_loss_price = entry_price * (1 - self.config.stop_loss_pct/100)
            target_price = entry_price * (1 + self.config.take_profit_pct/100)
            take_profit_price = target_price + 10

            logger.info(f"Attempting LONG position - Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")

            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side="long",
                size=size,
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)
            )

            if not success:
                logger.error("이미 포지션이 존재하기에 주문실패")
                self.entry_position_ratio = 0.0
            else:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed LONG limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")
            self.entry_position_ratio = 0.0

    async def execute_short_trade(self, current_price: float):
        """숏 포지션 Limit 진입 실행"""
        try:
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")

            # 현재 숏 비율 저장
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            self.entry_position_ratio = float(position_ratios.get('short_ratio', 0))
            logger.info(f"저장된 진입 시점 숏 비율: {self.entry_position_ratio}%")
            
            size = await self.calculate_position_size(current_price)
            if size == 0:
                return
            
            entry_price = current_price
            stop_loss_price = entry_price * (1 + self.config.stop_loss_pct/100)
            target_price = entry_price * (1 - self.config.take_profit_pct/100)
            take_profit_price = target_price - 10
            
            logger.info(f"Attempting SHORT position - Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
            
            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side="short",
                size=size,
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)
            )

            if not success:
                logger.error("이미 포지션이 존재하기에 주문실패")
                self.entry_position_ratio = 0.0
            else:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed SHORT limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing short trade: {e}")
            self.entry_position_ratio = 0.0

    async def should_close_position(self, position: Position, indicators: dict) -> Tuple[bool, str]:
        """포지션 청산 조건 확인"""
        try:
            if not isinstance(position, Position):
                position = await position
                if not position:
                    return False, ""
                
            current_price = float(indicators['last_close'])
            entry_price = position.entry_price
            break_even_price = position.break_even_price
            price_change = float(indicators['price_change'])
            
            logger.info(f"포지션 정보: 심볼={position.symbol}, "
                    f"방향={position.side}, "
                    f"크기={position.size}, "
                    f"진입가={position.entry_price}, "
                    f"레버리지={position.leverage}")
            
            # 포지션 비율 확인
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            
            # 롱/숏 포지션 비율이 진입 시점 대비 0.2% 이상 하락했는지 확인
            if position.side == 'long' and self.entry_position_ratio > 0:
                current_ratio = float(position_ratios.get('long_ratio', 0))
                if current_ratio < (self.entry_position_ratio - self.ratio_drop_threshold):
                    logger.info(f"롱 포지션 비율 하락 감지 (진입: {self.entry_position_ratio}% -> 현재: {current_ratio}%)")
                    return True, "ratio_drop"
                    
            elif position.side == 'short' and self.entry_position_ratio > 0:
                current_ratio = float(position_ratios.get('short_ratio', 0))
                if current_ratio < (self.entry_position_ratio - self.ratio_drop_threshold):
                    logger.info(f"숏 포지션 비율 하락 감지 (진입: {self.entry_position_ratio}% -> 현재: {current_ratio}%)")
                    return True, "ratio_drop"
            
            # PNL% 계산
            if position.side == 'long':
                pnl_percentage = ((current_price - break_even_price) / entry_price) * 100
            else:
                pnl_percentage = ((break_even_price - current_price) / entry_price) * 100
                
            logger.info(f"현재 PNL%: {pnl_percentage:.2f}%")
            
            # 손절 조건 (-0.3% 이하)
            if pnl_percentage <= -0.3:
                logger.info(f"손절 조건 충족: PNL = {pnl_percentage:.2f}%")
                return True, "stop_loss"
            
            # 익절 조건
            if position.side == 'long':
                # 롱 포지션: 음봉이면서 수익률 0.45% 이상
                should_close = price_change < 0 and pnl_percentage >= 0.45
                
                if should_close:
                    return True, "take_profit"
                    
            else:
                # 숏 포지션: 양봉이면서 수익률 0.45% 이상
                should_close = price_change > 0 and pnl_percentage >= 0.45
                
                if should_close:
                    return True, "take_profit"
            
            return False, ""
                    
        except Exception as e:
            logger.error(f"포지션 청산 조건 확인 중 에러: {e}")
            return False, ""

    async def close_position(self, position: Position, reason: str = ""):
        """포지션 청산"""
        try:
            # ratio_drop인 경우 청산 시점의 비율 저장
            if reason == "ratio_drop":
                position_ratios = self.market_data.calculate_position_ratio_indicators()
                if position.side == 'long':
                    self.ratio_drop_value = position_ratios['long_ratio']
                else:
                    self.ratio_drop_value = position_ratios['short_ratio']
                self.ratio_drop_direction = position.side
                logger.info(f"비율 하락으로 인한 청산 시작: {position.side}, 하락 비율: {self.ratio_drop_value}%")

            # 기존 미체결 주문들 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            logger.info(f"Attempting to close position for reason: {reason}")
            
            # 시장가 청산 시도 (손절 또는 비율 하락인 경우)
            if reason in ["stop_loss", "ratio_drop"]:
                close_success = await self.order_executor.execute_market_close(position)
                if close_success:
                    logger.info(f"Position closed with market order ({reason})")
                    self.in_position = False
                    return True
            else:  # 익절의 경우 리밋 청산 시도
                limit_price = self.market_data.get_latest_price()
                close_success = await self.order_executor.execute_limit_close(position, limit_price)
                
                if close_success:
                    logger.info("Position closed with limit order")
                    self.in_position = False
                    return True
                else:
                    # 리밋 주문 실패 시 시장가 청산
                    logger.warning("Limit close failed, attempting market close")
                    market_close_success = await self.order_executor.execute_market_close(position)
                    if market_close_success:
                        logger.info("Position closed with market order (after limit failure)")
                        self.in_position = False
                        return True
            
            logger.error("Failed to close position")
            return False
            
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False

    async def run(self):
        """전략 실행 메인 루프"""
        try:
            while True:
                try:
                    await self._process_trading_logic()
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Trading logic error: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error in run method: {e}")

    async def _process_trading_logic(self):
        """트레이딩 로직 처리"""
        try:
            # 포지션 비율 데이터 업데이트
            await self.market_data.update_position_ratio("BTCUSDT")
            
            # 포지션 상태 확인
            position = await self.order_executor.get_position("BTCUSDT")   
            
            logger.info(f"현재 포지션 상태: {position if position else '포지션 없음'}")

            # 이전에 포지션이 있었는데 지금 없다면 수동 청산으로 간주
            if self.in_position and not position:
                logger.info("포지션이 외부에서 청산됨을 감지")
                self.in_position = False
                self.last_trade_time = int(time.time())
                await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
                return
            
            # 기술적 지표 계산
            indicators = self.market_data.calculate_technical_indicators()        
            if not indicators:
                logger.warning("지표가 계산되지 않음")
                return
                
            # 포지션 비율 로깅
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            logger.info(
                f"현재 포지션 비율 상태:\n"
                f"  롱 비율: {position_ratios.get('long_ratio', 0):.2f}%\n"
                f"  숏 비율: {position_ratios.get('short_ratio', 0):.2f}%\n"
                f"  롱숏 비율: {position_ratios.get('long_short_ratio', 1):.2f}\n"
                f"  5분 변화: {position_ratios.get('ratio_change_5m', 0):.2f}\n"
                f"  진입 비율: {self.entry_position_ratio:.2f}%\n"
                f"  하락 임계값: {self.ratio_drop_threshold:.2f}%"
            )
                
            current_price = indicators.get('last_close')
            if not current_price:
                return
                
            current_time = int(time.time())
            
            if position is not None and position.size > 0:
                logger.info(f"포지션 존재. 사이즈: {position.size}, 방향: {position.side}")
                should_close, close_reason = await self.should_close_position(position, indicators)
                if should_close:
                    await self.close_position(position, close_reason)
            else:
                logger.info("포지션 없음. 신규 진입 조건 확인 중...")
                if (current_time - self.last_trade_time) >= self.min_trade_interval:
                    logger.info("진입 조건 체크 시작...")
                    
                    logger.info("롱 진입 조건 확인 중...")
                    if self.should_open_long(indicators):
                        logger.info("롱 진입 조건 충족 - 주문 실행")
                        await self.execute_long_trade(current_price)
                    else:
                        logger.info("숏 진입 조건 확인 중...")
                        if self.should_open_short(indicators):
                            logger.info("숏 진입 조건 충족 - 주문 실행")
                            await self.execute_short_trade(current_price)
                        else:
                            logger.info("진입 조건 미충족")
                else:
                    wait_time = self.min_trade_interval - (current_time - self.last_trade_time)
                    logger.info(f"진입 대기 중... (다음 진입까지 {wait_time}초)")
                        
        except Exception as e:
            logger.error(f"Error in trading logic: {e}")