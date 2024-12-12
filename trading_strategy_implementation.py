import logging
from typing import Optional, Tuple
from dataclasses import dataclass
import asyncio
import time
from order_execution import OrderExecutor
from market_data_manager import MarketDataManager
from models import Position

logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    leverage: int = 10
    stop_loss_pct: float = 0.2
    take_profit_pct: float = 1.5
    volume_threshold: float = 20.0
    stoch_rsi_high: float = 90.0
    stoch_rsi_low: float = 10.0
    position_size_pct: float = 100.0

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.in_position = False
        self.last_volume = 0.0
        self.last_trade_time = 0
        self.min_trade_interval = 60
        self.position_entry_timeout = 30  # 진입 주문 타임아웃
        self.position_close_timeout = 20  # 청산 주문 타임아웃

    async def calculate_position_size(self, current_price: float) -> float:
        """계좌 잔고를 기반으로 포지션 크기 계산"""
        try:
            account_info = self.order_executor.api.get_account_balance()
            logger.info(f"Account info received: {account_info}")
            
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
            
            available_balance = float(account_info['data']['available'])
            logger.info(f"Available balance: {available_balance}")
            
            trade_amount = available_balance * (self.config.position_size_pct / 100)
            position_size = (trade_amount * self.config.leverage) / current_price
            floor_size = float(str(position_size)[:5])
            
            logger.info(f"Original size: {position_size}, Floor size: {floor_size}")
            return floor_size
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0

    def should_open_long(self, indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) < float(self.config.stoch_rsi_low)
            price_above_ema = float(indicators['last_close']) > float(indicators['ema200'])
            price_falling = float(indicators['price_change']) < 0
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_above_ema and 
                price_falling and 
                not self.in_position
            )
                
            logger.info(f"롱 진입 조건 충족 여부: {should_enter}")
            return should_enter
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False

    def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        try:
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_high)
            price_below_ema = float(indicators['last_close']) < float(indicators['ema200'])
            price_rising = float(indicators['price_change']) > 0
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_below_ema and 
                price_rising and 
                not self.in_position
            )
                
            logger.info(f"숏 진입 조건 충족 여부: {should_enter}")
            return should_enter
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False

    async def execute_long_trade(self, current_price: float):
        """롱 포지션 Limit 진입 실행"""
        try:
            # 기존 미체결 주문 확인 및 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            size = await self.calculate_position_size(current_price)
            logger.info(f"Calculated position size: {size}")
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
            
            if success:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed LONG limit order at {entry_price}")
            else:
                logger.error("Failed to place LONG position")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")

    async def execute_short_trade(self, current_price: float):
        """숏 포지션 Limit 진입 실행"""
        try:
            # 기존 미체결 주문 확인 및 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            size = await self.calculate_position_size(current_price)
            logger.info(f"Calculated position size: {size}")
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
            
            if success:
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed SHORT limit order at {entry_price}")
            else:
                logger.error("Failed to place SHORT position")
                
        except Exception as e:
            logger.error(f"Error executing short trade: {e}")

    async def should_close_position(self, position: Position, indicators: dict) -> Tuple[bool, str]:
        """
        포지션 청산 조건 확인
        Returns: (bool, str) - (청산해야 하는지 여부, 청산 이유)
        """
        try:
            current_price = float(indicators['last_close'])
            entry_price = position.entry_price
            
            logger.info(f"포지션 정보: 심볼={position.symbol}, "
                    f"방향={position.side}, "
                    f"크기={position.size}, "
                    f"진입가={position.entry_price}, "
                    f"레버리지={position.leverage}")
            
            # PNL% 계산
            if position.side == 'long':
                pnl_percentage = ((current_price - entry_price) / entry_price) * 100
            else:
                pnl_percentage = ((entry_price - current_price) / entry_price) * 100
                
            logger.info(f"현재 PNL%: {pnl_percentage:.2f}%")
            
            # 손절 조건 (-0.2% 이하)
            if pnl_percentage <= -0.2:
                logger.info(f"손절 조건 충족: PNL = {pnl_percentage:.2f}%")
                return True, "stop_loss"
            
            # ATR 기반 이익실현 조건 체크
            atr = self.market_data.calculate_atr(period=14)
            logger.info(f"현재 ATR: {atr:.2f}")
            
            # ATR 기반 이동폭 계산
            if atr < 100:
                price_move = atr * 6
                logger.info(f"ATR < 100, 이동폭 = ATR × 6 = {price_move:.2f}")
            elif atr <= 200:
                price_move = atr * 4
                logger.info(f"100 ≤ ATR ≤ 200, 이동폭 = ATR × 4 = {price_move:.2f}")
            else:
                price_move = atr * 5
                logger.info(f"ATR > 200, 이동폭 = ATR × 5.0 = {price_move:.2f}")
                
            if position.side == 'long':
                target_price = entry_price + price_move
                should_close = current_price >= target_price
                logger.info(f"롱 포지션 상태: "
                        f"진입가={entry_price:.2f}, "
                        f"목표가={target_price:.2f}, "
                        f"현재가={current_price:.2f}, "
                        f"청산조건={'충족' if should_close else '미충족'}")
                if should_close:
                    return True, "take_profit"
                    
            else:
                target_price = entry_price - price_move
                should_close = current_price <= target_price
                logger.info(f"숏 포지션 상태: "
                        f"진입가={entry_price:.2f}, "
                        f"목표가={target_price:.2f}, "
                        f"현재가={current_price:.2f}, "
                        f"청산조건={'충족' if should_close else '미충족'}")
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
            
            # 시장가 청산 시도
            if reason == "stop_loss":
                close_success = await self.order_executor.execute_market_close(position)
                if close_success:
                    logger.info("Position closed with market order (stop loss)")
                    self.in_position = False
                    return True
            else:
                # 리밋 청산 시도
                current_price = self.market_data.get_latest_price()
                close_success = await self.order_executor.execute_limit_close(position, current_price)
                
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
            order_check_task = asyncio.create_task(
                self.order_executor.check_pending_orders()
            )
            
            while True:
                try:
                    await self._process_trading_logic()
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Trading logic error: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error in run method: {e}")
        finally:
            if order_check_task and not order_check_task.done():
                order_check_task.cancel()

    async def _process_trading_logic(self):
        """트레이딩 로직 처리"""
        try:
            # 포지션 상태 확인
            position = self.order_executor.get_position("BTCUSDT")
            
            # 이전에 포지션이 있었는데 지금 없다면 수동 청산으로 간주
            if self.in_position and not position:
                logger.info("포지션이 외부에서 청산됨을 감지")
                self.in_position = False
                self.last_trade_time = int(time.time())
                # 남은 미체결 주문 정리
                await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
                return
            
            # 기술적 지표 계산
            indicators = self.market_data.calculate_technical_indicators()
            if not indicators:
                logger.warning("지표가 계산되지 않음")
                return
                
            current_price = indicators.get('last_close')
            if not current_price:
                return
                
            current_time = int(time.time())
            
            if position:
                # 포지션이 있는 경우 청산 조건 확인
                should_close, close_reason = await self.should_close_position(position, indicators)
                if should_close:
                    await self.close_position(position, close_reason)
            else:
                # 새로운 포지션 진입 평가
                if (current_time - self.last_trade_time) >= self.min_trade_interval:
                    if self.should_open_long(indicators):
                        logger.info("롱 진입 조건 충족 - 주문 실행")
                        await self.execute_long_trade(current_price)
                    elif self.should_open_short(indicators):
                        logger.info("숏 진입 조건 충족 - 주문 실행")
                        await self.execute_short_trade(current_price)
                        
        except Exception as e:
            logger.error(f"Error in trading logic: {e}")

    async def sync_position_status(self):
        """포지션 상태 동기화"""
        position = self.order_executor.get_position("BTCUSDT")
        self.in_position = bool(position)
        return self.in_position