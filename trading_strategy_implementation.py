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
            price_above_ema = float(indicators['last_close']) > float(indicators['ema200'])
            price_rising = float(indicators['price_change']) > 0 # 양봉에 진입하는게 맞다고 본다. 음봉에 진입하면 보통 횡보장에서 박스하강에 자주당함.
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_above_ema and 
                price_rising and 
                not self.in_position
            )
                
            logger.info(f"롱 진입 조건 충족 여부: {should_enter}")
            return should_enter             # should_enter() 값들이 전부 True가 되어야 True로 반환하는거다. and 조건 연산자 활용.
                
        except KeyError as e:
            logger.error(f"Missing indicator: {e}")
            return False

    def should_open_short(self, indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        logger.info("숏 포지션 진입 조건 확인 시작")  # 함수 시작 확인용 로그
        try:
            # 디버깅을 위해 indicators 내용 출력
            logger.info(f"Received indicators: {indicators}")
            
            volume_surge = float(indicators['last_volume']) > float(self.config.volume_threshold)
            stoch_rsi_condition = float(indicators['stoch_k']) > float(self.config.stoch_rsi_high)
            price_below_ema = float(indicators['last_close']) < float(indicators['ema200'])
            price_falling = float(indicators['price_change']) < 0

            # 상세 조건 로깅
            logger.info(f"Short Entry Conditions:\n"
                    f"  Volume ({indicators['last_volume']:.2f} > {self.config.volume_threshold}): {volume_surge}\n"
                    f"  Stoch RSI K ({indicators['stoch_k']:.2f} > {self.config.stoch_rsi_high}): {stoch_rsi_condition}\n"
                    f"  Price Below EMA200 ({indicators['last_close']:.2f} < {indicators['ema200']:.2f}): {price_below_ema}\n"
                    f"  Price Falling ({indicators['price_change']:.2f} < 0): {price_falling}\n"
                    f"  No Position: {not self.in_position}")
            
            should_enter = (
                volume_surge and 
                stoch_rsi_condition and 
                price_below_ema and 
                price_falling and 
                not self.in_position
            )
            
            logger.info(f"최종 숏 진입 결정: {should_enter}")
            return should_enter
                    
        except KeyError as e:
            logger.error(f"Missing indicator in should_open_short: {e}\nIndicators received: {indicators}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in should_open_short: {e}")
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
                    # 이부분이 원래는 not이 없었음. 그래서 제대로 주문이 안들어간듯 싶다. not이 없다면... 이미 포지션이 존재한다는 뜻이 아니라 성공해서 포지션이 존재한다는 뜻이되어버림.
            if not success:
                logger.error("이미 포지션이 존재하기에 주문실패")
            else:
                self.in_position = True # 이렇게 해야만, 성공해서 포지션이 존재하게 됐다는 뜻이 됨.
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed SHORT limit order at {entry_price}")
                
        except Exception as e:
            logger.error(f"Error executing long trade: {e}")

    async def should_close_position(self, position: Position, indicators: dict) -> Tuple[bool, str]:
        """                                                                                                  
        포지션 청산 조건 확인
        Returns: (bool, str) - (청산해야 하는지 여부, 청산 이유)
        """
        try:
            if not isinstance(position, Position):
                position = await position  # await 추가
                if not position:  # position이 None인 경우 처리
                    return False, ""
                
            current_price = float(indicators['last_close'])
            entry_price = position.entry_price
            break_even_price = position.break_even_price
            price_change = float(indicators['price_change'])  # 양봉/음봉 확인용
            
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
            
            # 손절 조건 (-0.3% 이하)
            if pnl_percentage <= -0.3:
                logger.info(f"손절 조건 충족: PNL = {pnl_percentage:.2f}%")
                return True, "stop_loss"
            
            # 새로운 익절 조건
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

    async def _process_trading_logic(self):
        """트레이딩 로직 처리"""
        try:
            # 포지션 상태 확인 (await 추가)
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
            if not current_price:
                return
                
            current_time = int(time.time())
            
            if position is not None and position.size > 0:  # position은 이미 await 완료됨
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