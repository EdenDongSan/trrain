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

    async def calculate_position_size(self, current_price: float) -> float:                 # 숏이나 롱포지션에 주문 넣으려고 할때 초반에 쓰인다.
        """계좌 잔고를 기반으로 포지션 크기 계산"""
        try:
            account_info = self.order_executor.api.get_account_balance()                #api로 계좌잔고정보를 호출하는 부분이다.
            logger.info(f"Account info received: {account_info}")
            
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
            
            available_balance = float(account_info['data']['available'])             #계좌잔고 정보를 변수로 설정한다.
            logger.info(f"Available balance: {available_balance}")
            
            trade_amount = available_balance * (self.config.position_size_pct / 100)           #이건 컨피그로 어느정도의 사이즈로 진입할지 결정하는 부분이다. 계좌잔고 변수. 기반 퍼센트.
            position_size = (trade_amount * self.config.leverage) / current_price
            floor_size = math.floor(position_size * 1000) / 1000                #포지션사이즈를 bitget양식에 맞춰서 소수점 셋째자리까지 관리해주는 부분이다. math라이브러리사용.
            
            logger.info(f"Original size: {position_size}, Floor size: {floor_size}")
            return floor_size
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0

    def should_open_long(self, indicators: dict) -> bool:               #결국 롱포지션진입을 하기직전에 호출하는 함수다. 불값으로 true, false를 반환한다.
        """롱 포지션 진입 조건 확인"""
        try:
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) < float(self.config.stoch_rsi_low)
            price_above_ema = float(indicators['last_close']) < float(indicators['ema200'])
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

    def should_open_short(self, indicators: dict) -> bool:                     #결국 숏숏포지션진입을 하기직전에 호출하는 함수다. 불값으로 true, false를 반환한다.
        """숏 포지션 진입 조건 확인"""
        try:
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_high)
            price_below_ema = float(indicators['last_close']) > float(indicators['ema200'])
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

    async def execute_long_trade(self, current_price: float):              # 불값으로 true를 받고 비동기로 실행이 된다.
        """롱 포지션 Limit 진입 실행"""
        try:
            # 기존 미체결 주문 확인 및 취소
            await self.order_executor.cancel_all_symbol_orders("BTCUSDT")
            
            size = await self.calculate_position_size(current_price) 
            logger.info(f"Calculated position size: {size}")
            if size == 0:
                return
            
            entry_price = current_price                                # 주문할때 필요한 변수들을 설정한다.
            stop_loss_price = entry_price * (1 - self.config.stop_loss_pct/100)        # 컨피그에 tp/sl 부분 하드코딩하는 부분이 있다.
            target_price = entry_price * (1 + self.config.take_profit_pct/100)
            take_profit_price = target_price + 10  # 병;신같은데 이거 수정할까.. 고정값 하드코딩이라..
            
            logger.info(f"Attempting LONG position - Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
            #order_executor에 open_position을 일단호출한다. 값은 success값으로 넘겨준다.
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
            # 이부분이 원래는 not이 없었음. 그래서 제대로 주문이 안들어간듯 싶다. not이 없다면... 이미 포지션이 존재한다는 뜻이 아니라 성공해서 포지션이 존재한다는 뜻이되어버림.
            if not success:
                logger.error("이미 포지션이 존재하기에 주문실패")
            else:
                self.in_position = True # 이렇게 해야만, 성공해서 포지션이 존재하게 됐다는 뜻이 됨.
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed LONG limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")

    async def execute_short_trade(self, current_price: float):                 # 불값으로 true를 받고 비동기로 실행이 된다.
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

    async def should_close_position(self, position: Position, indicators: dict) -> Tuple[bool, str]:         # 여기 있는 포지션은 order_exection의 겟포지션에서 받아오게 되어있다.
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
                price_move = atr * 8
                logger.info(f"ATR < 100, 이동폭 = ATR × 8 = {price_move:.2f}")
            elif atr <= 200:
                price_move = atr * 4
                logger.info(f"100 ≤ ATR ≤ 200, 이동폭 = ATR × 4 = {price_move:.2f}")
            else:
                price_move = atr * 3
                logger.info(f"ATR > 200, 이동폭 = ATR × 3.0 = {price_move:.2f}")
                
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

    async def close_position(self, position: Position, reason: str = ""):        # 프로세스 트레이딩 로직함수 안에서 작동됨. await로 대기중.
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
            else: # 이때는 반환하는 튜플값에 true, "take_profit" 이라고 전달된다. 즉, 익절주문은 리밋 청산을 시도한다는 것임.
                # 리밋 청산 시도       
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
                    await self._process_trading_logic()           #run으로 실행 시키는 함수.
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Trading logic error: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Error in run method: {e}")

    async def _process_trading_logic(self):         # 얘가 run 태스크로 무한 반복하는 함수. 무한루프함수.
        """트레이딩 로직 처리"""
        try:
            # 포지션 상태 확인
            position = self.order_executor.get_position("BTCUSDT")   #order_executor에 있는는 함수를 호출한다.
            
            # 이전에 포지션이 있었는데 지금 없다면 수동 청산으로 간주
            if self.in_position and not position:
                logger.info("포지션이 외부에서 청산됨을 감지")
                self.in_position = False
                self.last_trade_time = int(time.time())
                # 남은 미체결 주문 정리
                await self.order_executor.cancel_all_symbol_orders("BTCUSDT") #여기서 심볼을 주네..심볼관리는 여기서. 여기서 미체결 주문 관리를 한다. 30초 지난 것들 싹다 삭제. 심플.
                return
            
            # 기술적 지표 계산
            # 여기서 market_data_manager을 실행시킴. 인디케이터들을 가져온다. (최근n개의 캔들데이터,이전봉대비 가격변동,ema7,25,200,stoch rsi k값d값)
            indicators = self.market_data.calculate_technical_indicators()        
            if not indicators:
                logger.warning("지표가 계산되지 않음")
                return
                
            current_price = indicators.get('last_close')
            if not current_price:
                return
                
            current_time = int(time.time())
            
            if position:
                # 포지션이 있는 경우 청산조건만! 확인.
                should_close, close_reason = await self.should_close_position(position, indicators)  # 청산조건호출함수. true, "stop_loss" or "take_profit" 튜플값으로 반환.
                if should_close:
                    await self.close_position(position, close_reason) #포지션청산함수.
            else:
                # 새로운 포지션 진입 평가
                if (current_time - self.last_trade_time) >= self.min_trade_interval:
                    if self.should_open_long(indicators):                 #롱 진입 신호 호출 함수. 불값이다.
                        logger.info("롱 진입 조건 충족 - 주문 실행")
                        await self.execute_long_trade(current_price)
                    elif self.should_open_short(indicators):             #숏 진입 신호 호출 함수. 불값이다.
                        logger.info("숏 진입 조건 충족 - 주문 실행")
                        await self.execute_short_trade(current_price)
                        
        except Exception as e:
            logger.error(f"Error in trading logic: {e}")