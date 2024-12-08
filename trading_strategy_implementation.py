import logging
from typing import Optional
from dataclasses import dataclass
from decimal import Decimal
import asyncio
import time  # 추가
from order_execution import OrderExecutor
from market_data_manager import MarketDataManager

logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    leverage: int = 5
    stop_loss_pct: float = 2.0
    take_profit_pct: float = 5.0
    volume_threshold: float = 100.0
    stoch_rsi_high: float = 90.0
    stoch_rsi_low: float = 10.0
    position_size_pct: float = 100.0  # 계좌 잔고의 몇 %를 사용할지

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.in_position = False
        self.last_volume = 0.0
        self.last_trade_time = 0
        self.min_trade_interval = 300  # 최소 거래 간격 (초)
        
    async def calculate_position_size(self, current_price: float) -> float:
        """
        계좌 잔고를 기반으로 포지션 크기 계산
        """
        try:
            # 계좌 잔고 조회
            account_info = await self.order_executor.api.get_account_balance()
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
                
            # 사용 가능한 잔고 추출
            available_balance = float(account_info['data']['available'])
            
            # 설정된 비율만큼의 금액 계산
            trade_amount = available_balance * (self.config.position_size_pct / 100)
            
            # 레버리지를 고려한 실제 포지션 크기 계산 (BTC 수량)
            position_size = (trade_amount * self.config.leverage) / current_price
            
            # BTC는 3자리까지 반올림
            return round(position_size, 3)
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0
        
    def should_open_long(self, indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            # 모든 값을 float으로 변환하여 비교
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) < float(self.config.stoch_rsi_low)
            price_above_ema = float(indicators['last_close']) > float(indicators['ema200'])
            price_falling = float(indicators['price_change']) < 0
            
            return (volume_surge and stoch_rsi_condition and 
                price_above_ema and price_falling and not self.in_position)
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False
                
    def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        try:
            # 모든 값을 float으로 변환하여 비교
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_high)
            price_below_ema = float(indicators['last_close']) < float(indicators['ema200'])
            price_rising = float(indicators['price_change']) > 0
            
            return (volume_surge and stoch_rsi_condition and 
                price_below_ema and price_rising and not self.in_position)
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False
        
    async def execute_long_trade(self, current_price: float):
        """롱 포지션 Limit 진입 실행"""
        try:
            size = await self.calculate_position_size(current_price)
            if size == 0:
                return
                
            # 진입 가격: 현재가 - $20
            entry_price = current_price - 20
            
            # 스탑로스 가격 계산 (-2%)
            stop_loss_price = entry_price * (1 - self.config.stop_loss_pct/100)
            
            # 테이크프로핏 가격 계산 (+5% + $10)
            target_price = entry_price * (1 + self.config.take_profit_pct/100)
            take_profit_price = target_price + 10
            
            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side="long",
                size=size,
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)  # Limit 주문 가격 추가
            )
            
            if success:
                self.in_position = True
                logger.info(f"Placed LONG limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")

    async def execute_short_trade(self, current_price: float):
        """숏 포지션 Limit 진입 실행"""
        try:
            size = await self.calculate_position_size(current_price)
            if size == 0:
                return
                
            # 진입 가격: 현재가 + $20
            entry_price = current_price + 20
            
            # 스탑로스 가격 계산 (+2%)
            stop_loss_price = entry_price * (1 + self.config.stop_loss_pct/100)
            
            # 테이크프로핏 가격 계산 (-5% - $10)
            target_price = entry_price * (1 - self.config.take_profit_pct/100)
            take_profit_price = target_price - 10
            
            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side="short",
                size=size,
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)  # Limit 주문 가격 추가
            )
            
            if success:
                self.in_position = True
                logger.info(f"Placed SHORT limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing short trade: {e}")

    async def run(self):
        """전략 실행 메인 루프"""
        try:
            # 미체결 주문 체크 태스크 시작
            order_check_task = asyncio.create_task(
                self.order_executor.check_pending_orders()
            )
            
            while True:
                try:
                    # 기술적 지표 계산
                    indicators = self.market_data.calculate_technical_indicators()
                    if not indicators:
                        await asyncio.sleep(1)
                        continue
                        
                    current_price = indicators.get('last_close')
                    if not current_price:
                        continue
                        
                    current_time = int(time.time())
                    
                    # 포지션 상태 업데이트
                    if self.in_position:
                        position = await self.order_executor.get_position("BTCUSDT")
                        if not position:
                            self.in_position = False
                            self.last_trade_time = current_time
                    
                    # 최소 거래 간격 확인
                    if not self.in_position and (current_time - self.last_trade_time) >= self.min_trade_interval:
                        # 진입 조건 확인
                        if self.should_open_long(indicators):
                            await self.execute_long_trade(current_price)
                            self.last_trade_time = current_time
                        elif self.should_open_short(indicators):
                            await self.execute_short_trade(current_price)
                            self.last_trade_time = current_time
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error in strategy loop: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in run method: {e}")
        finally:
            # 태스크 정리
            if order_check_task and not order_check_task.done():
                order_check_task.cancel()
