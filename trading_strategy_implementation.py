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
    leverage: int = 20
    stop_loss_pct: float = 0.4
    take_profit_pct: float = 1.5
    volume_threshold: float = 20.0
    stoch_rsi_high: float = 5.0
    stoch_rsi_low: float = 95.0
    position_size_pct: float = 95.0

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.in_position = False
        self.last_volume = 0.0
        self.last_trade_time = 0
        self.min_trade_interval = 120
        
        # 롱/숏 진입 시점의 비율과 제한 관련 변수들
        self.long_entry_ratio = 0.0   # 롱 진입 시점의 롱 계좌 비율
        self.short_entry_ratio = 0.0  # 숏 진입 시점의 숏 계좌 비율
        self.long_ratio_lock = False  # 롱 진입 제한 플래그
        self.short_ratio_lock = False # 숏 진입 제한 플래그
        self.last_price_above_ema = True  # 롱을 위한 EMA 트래킹
        self.last_price_below_ema = True  # 숏을 위한 EMA 트래킹

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

    def should_open_long(self, indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            # 현재 롱 비율 확인
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            current_long_ratio = float(position_ratios.get('long_ratio', 0))

            # 락이 걸려있고, 현재 비율이 진입 시점 비율 미만으로 감소한 경우
            if self.long_ratio_lock and current_long_ratio <= self.long_entry_ratio:
                self.long_ratio_lock = False  # 락 해제
                logger.info(f"롱 비율이 감소함 ({current_long_ratio:.4f}% <= {self.long_entry_ratio:.4f}%). 락 해제됨")

            # 이전 진입 시점 비율보다 현재 비율이 0.002% 이상 높으면 진입 제한
            if self.long_entry_ratio > 0 and (current_long_ratio - self.long_entry_ratio) >= 0.002:
                self.long_ratio_lock = True
                logger.info(f"전체 롱 비율이 진입 시점({self.long_entry_ratio:.4f}%)보다 0.002% 이상 높음({current_long_ratio:.4f}%). 진입 제한")
                return False

            # 락 상태 체크
            if self.long_ratio_lock:
                logger.info("롱 진입 제한 상태")
                return False

            # 나머지 진입 조건들은 동일하게 유지
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_low)
            price_below_ema = float(indicators['last_close']) < float(indicators['ema200'])
            price_rising = float(indicators['price_change']) > 0
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_below_ema and 
                price_rising and 
                not self.in_position
            )
                
            logger.info(f"Long Entry Conditions:\n"
                    f"  Volume ({indicators['last_volume']:.4f} > {self.config.volume_threshold:.4f}): {volume_surge}\n"
                    f"  Stoch RSI K ({indicators['stoch_k']:.4f} > {self.config.stoch_rsi_low:.4f}): {stoch_rsi_condition}\n"
                    f"  Price Below EMA200 ({indicators['last_close']:.4f} < {indicators['ema200']:.4f}): {price_below_ema}\n"
                    f"  Price Rising ({indicators['price_change']:.4f} > 0): {price_rising}\n"
                    f"  Current Long Ratio: {current_long_ratio:.4f}%\n"
                    f"  Entry Long Ratio: {self.long_entry_ratio:.4f}%\n"
                    f"  No Position: {not self.in_position}")
                
            return should_enter
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in should_open_long: {e}")
            return False

    def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        try:
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            current_short_ratio = float(position_ratios.get('short_ratio', 0))

            # 락이 걸려있고, 현재 비율이 진입 시점 비율 미만으로 감소한 경우
            if self.short_ratio_lock and current_short_ratio <= self.short_entry_ratio:
                self.short_ratio_lock = False
                logger.info(f"숏 비율이 감소함 ({current_short_ratio:.4f}% <= {self.short_entry_ratio:.4f}%). 락 해제됨")

            # 이전 진입 시점 비율보다 현재 비율이 0.002% 이상 높으면 진입 제한
            if self.short_entry_ratio > 0 and (current_short_ratio - self.short_entry_ratio) >= 0.002:
                self.short_ratio_lock = True
                logger.info(f"전체 숏 비율이 진입 시점({self.short_entry_ratio:.4f}%)보다 0.002% 이상 높음({current_short_ratio:.4f}%). 진입 제한")
                return False

            if self.short_ratio_lock:
                logger.info("숏 진입 제한 상태")
                return False

            # 나머지 진입 조건들은 동일하게 유지
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) < float(self.config.stoch_rsi_high)
            price_above_ema = float(indicators['last_close']) > float(indicators['ema200'])
            price_falling = float(indicators['price_change']) < 0

            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_above_ema and 
                price_falling and 
                not self.in_position
            )

            logger.info(f"Short Entry Conditions:\n"
                    f"  Volume ({indicators['last_volume']:.4f} > {self.config.volume_threshold:.4f}): {volume_surge}\n"
                    f"  Stoch RSI K ({indicators['stoch_k']:.4f} < {self.config.stoch_rsi_high:.4f}): {stoch_rsi_condition}\n"
                    f"  Price Above EMA200 ({indicators['last_close']:.4f} > {indicators['ema200']:.4f}): {price_above_ema}\n"
                    f"  Price Falling ({indicators['price_change']:.4f} < 0): {price_falling}\n"
                    f"  Current Short Ratio: {current_short_ratio:.4f}%\n"
                    f"  Entry Short Ratio: {self.short_entry_ratio:.4f}%\n"
                    f"  No Position: {not self.in_position}")
            
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

            # 진입 시점의 롱 계좌 비율 저장 - 오류 처리 추가
            try:
                position_ratios = self.market_data.calculate_position_ratio_indicators()
                new_ratio = float(position_ratios.get('long_ratio', 0))
                
                # 유효한 비율값인지 확인
                if 0 <= new_ratio <= 100:
                    # 이전 비율값이 있고 락이 걸리지 않은 상태라면 새로운 비율로 업데이트
                    if self.long_entry_ratio == 0 or not self.long_ratio_lock:
                        self.long_entry_ratio = new_ratio
                        logger.info(f"롱 진입 시점의 전체 롱 비율 저장: {self.long_entry_ratio}%")
                    else:
                        logger.info(f"기존 롱 비율 유지: {self.long_entry_ratio}% (현재 락 상태)")
                else:
                    logger.error(f"유효하지 않은 롱 비율: {new_ratio}%")
                    return
                    
            except Exception as e:
                logger.error(f"롱 비율 저장 중 오류 발생: {e}")
                return
                
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
                logger.error("이미 포지션이 존재하거나 주문 실패")
                # 주문 실패 시 비율값 원복 (새로 설정한 경우에만)
                if not self.long_ratio_lock and self.long_entry_ratio == new_ratio:
                    self.long_entry_ratio = 0.0
            else:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed LONG limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")
            # 예외 발생 시 비율값 원복 (새로 설정한 경우에만)
            if not self.long_ratio_lock and hasattr(self, 'new_ratio'):
                self.long_entry_ratio = 0.0

    async def execute_short_trade(self, current_price: float):
        """숏 포지션 Limit 진입 실행"""
        try:
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")

            # 진입 시점의 숏 계좌 비율 저장 - 오류 처리 추가
            try:
                position_ratios = self.market_data.calculate_position_ratio_indicators()
                new_ratio = float(position_ratios.get('short_ratio', 0))
                
                # 유효한 비율값인지 확인
                if 0 <= new_ratio <= 100:
                    # 이전 비율값이 있고 락이 걸리지 않은 상태라면 새로운 비율로 업데이트
                    if self.short_entry_ratio == 0 or not self.short_ratio_lock:
                        self.short_entry_ratio = new_ratio
                        logger.info(f"숏 진입 시점의 전체 숏 비율 저장: {self.short_entry_ratio}%")
                    else:
                        logger.info(f"기존 숏 비율 유지: {self.short_entry_ratio}% (현재 락 상태)")
                else:
                    logger.error(f"유효하지 않은 숏 비율: {new_ratio}%")
                    return
                    
            except Exception as e:
                logger.error(f"숏 비율 저장 중 오류 발생: {e}")
                return
                
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
                logger.error("이미 포지션이 존재하거나 주문 실패")
                # 주문 실패 시 비율값 원복 (새로 설정한 경우에만)
                if not self.short_ratio_lock and self.short_entry_ratio == new_ratio:
                    self.short_entry_ratio = 0.0
            else:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed SHORT limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing short trade: {e}")
            # 예외 발생 시 비율값 원복 (새로 설정한 경우에만)
            if not self.short_ratio_lock and hasattr(self, 'new_ratio'):
                self.short_entry_ratio = 0.0

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
            
            # 현재 포지션 비율 확인
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            current_long_ratio = float(position_ratios.get('long_ratio', 0))
            current_short_ratio = float(position_ratios.get('short_ratio', 0))
            
            # 포지션 방향에 따른 비율 체크 및 즉시 청산, 락 설정
            if position.side == 'long':
                ratio_drop = self.long_entry_ratio - current_long_ratio
                if self.long_entry_ratio > 0 and ratio_drop >= 0.003:
                    self.long_ratio_lock = True  # 락 설정
                    logger.info(f"롱 포지션 비율 하락으로 락 설정 및 청산 (진입: {self.long_entry_ratio:.4f}%, 현재: {current_long_ratio:.4f}%, 하락폭: {ratio_drop:.4f}%)")
                    return True, "ratio_drop"
            else:  # short position
                ratio_drop = self.short_entry_ratio - current_short_ratio
                if self.short_entry_ratio > 0 and ratio_drop >= 0.003:
                    self.short_ratio_lock = True  # 락 설정
                    logger.info(f"숏 포지션 비율 하락으로 락 설정 및 청산 (진입: {self.short_entry_ratio:.4f}%, 현재: {current_short_ratio:.4f}%, 하락폭: {ratio_drop:.4f}%)")
                    return True, "ratio_drop"
            
            logger.info(f"포지션 정보: 심볼={position.symbol}, "
                    f"방향={position.side}, "
                    f"크기={position.size}, "
                    f"진입가={position.entry_price}, "
                    f"레버리지={position.leverage}")
            
            # PNL% 계산
            if position.side == 'long':
                pnl_percentage = ((current_price - break_even_price) / entry_price) * 100
            else:
                pnl_percentage = ((break_even_price - current_price) / entry_price) * 100
                
            logger.info(f"현재 PNL%: {pnl_percentage:.2f}%")
            
            # 손절 조건 (-0.4% 이하)
            if pnl_percentage <= -0.4:
                logger.info(f"손절 조건 충족: PNL = {pnl_percentage:.2f}%")
                return True, "stop_loss"
            
            # 익절 조건
            if position.side == 'long':
                # 롱 포지션: 음봉이면서 수익률 0.6% 이상
                should_close = price_change < 0 and pnl_percentage >= 0.6
                
                if should_close:
                    return True, "take_profit"
                    
            else:
                # 숏 포지션: 양봉이면서 수익률 0.6% 이상
                should_close = price_change > 0 and pnl_percentage >= 0.6
                
                if should_close:
                    return True, "take_profit"
            
            return False, ""
                    
        except Exception as e:
            logger.error(f"포지션 청산 조건 확인 중 에러: {e}")
            return False, ""

    async def close_position(self, position: Position, reason: str = ""):
        """포지션 청산"""
        try:
            # 기존 미체결 주문들 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            logger.info(f"Attempting to close position for reason: {reason}")
            
            # 시장가 청산 조건: 손절이거나 비율 하락일 경우
            if reason in ["stop_loss", "ratio_drop"]:
                close_success = await self.order_executor.execute_market_close(position)
                if close_success:
                    logger.info(f"Position closed with market order ({reason})")
                    self.in_position = False
                    
                    # 비율 하락으로 인한 청산의 경우 락은 이미 설정되어 있고 
                    # 비율 초기화는 200EMA 돌파 시에만 하므로 여기서는 하지 않음
                    if reason == "ratio_drop":
                        if position.side == 'long':
                            logger.info(f"롱 포지션 비율 하락으로 청산 완료. 락 상태 유지중 (락 해제는 200EMA 하향 돌파 시)")
                        else:
                            logger.info(f"숏 포지션 비율 하락으로 청산 완료. 락 상태 유지중 (락 해제는 200EMA 상향 돌파 시)")
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
                
            current_price = indicators.get('last_close')
            ema200 = indicators.get('ema200')
            if not (current_price and ema200):
                return

            # 포지션이 없을 때만 200 EMA 돌파 체크 및 비율 초기화 로직 실행
            if position is None or position.size == 0:
                # 200 EMA 돌파 체크
                price_above_ema = current_price > ema200
                if price_above_ema != self.last_price_above_ema:  # EMA 돌파 발생
                    if price_above_ema:  # 상향 돌파
                        logger.info(f"가격이 200 EMA를 상향 돌파 (가격: {current_price:.2f} > EMA200: {ema200:.2f})")
                        # 숏 진입 비율 초기화 및 락 해제
                        if self.short_ratio_lock:
                            self.short_ratio_lock = False
                            self.short_entry_ratio = 0.0
                            logger.info("200 EMA 상향 돌파로 숏 진입 비율 초기화 및 락 해제")
                    else:  # 하향 돌파
                        logger.info(f"가격이 200 EMA를 하향 돌파 (가격: {current_price:.2f} < EMA200: {ema200:.2f})")
                        # 롱 진입 비율 초기화 및 락 해제
                        if self.long_ratio_lock:
                            self.long_ratio_lock = False
                            self.long_entry_ratio = 0.0
                            logger.info("200 EMA 하향 돌파로 롱 진입 비율 초기화 및 락 해제")
                self.last_price_above_ema = price_above_ema
            else:
                logger.info("포지션 보유 중 - 비율 초기화 로직 스킵")

            # 포지션 비율 로깅
            position_ratios = self.market_data.calculate_position_ratio_indicators()
            logger.info(
                f"현재 포지션 비율 상태:\n"
                f"  롱 비율: {position_ratios.get('long_ratio', 0):.4f}%\n"
                f"  숏 비율: {position_ratios.get('short_ratio', 0):.4f}%\n"
                f"  롱숏 비율: {position_ratios.get('long_short_ratio', 1):.4f}\n"
                f"  5분 변화: {position_ratios.get('ratio_change_5m', 0):.4f}\n"
                f"  롱 진입 비율: {self.long_entry_ratio:.4f}%\n"
                f"  숏 진입 비율: {self.short_entry_ratio:.4f}%\n"
                f"  롱 락 상태: {self.long_ratio_lock}\n"
                f"  숏 락 상태: {self.short_ratio_lock}"
            )
                
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