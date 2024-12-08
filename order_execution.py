import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime
import asyncio
from data_api import BitgetAPI
import time

logger = logging.getLogger(__name__)

@dataclass
class Position:
    symbol: str
    side: str  # 'long' or 'short'
    size: float
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    timestamp: int
    leverage: int

class OrderExecutor:
    def __init__(self, api: BitgetAPI):
        self.api = api
        self.positions: Dict[str, Position] = {}
        self.pending_orders: Dict[str, Dict] = {}  # 미체결 주문 관리
        self.order_check_interval = 10  # 주문 체결 확인 간격 (초)
        
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """레버리지 설정"""
        try:
            # Bitget API에 레버리지 설정하는 코드 추가 필요
            return True
        except Exception as e:
            logger.error(f"Error setting leverage: {e}")
            return False
        
    async def check_pending_orders(self):
        """미체결 주문 상태 확인"""
        while True:
            try:
                for order_id, order_info in list(self.pending_orders.items()):
                    response = self.api.get_order_detail(order_info['symbol'], order_id)
                    
                    if response.get('code') == '00000':
                        status = response['data']['state']
                        
                        # 주문이 체결된 경우
                        if status == 'filled':
                            del self.pending_orders[order_id]
                            continue
                            
                        # 미체결 주문 처리
                        current_time = time.time()
                        order_time = order_info['timestamp']
                        
                        # 청산 주문인 경우 (20초 후 시장가 전환)
                        if order_info['is_close'] and current_time - order_time > 20:
                            logger.info(f"Converting close order to market order: {order_id}")
                            
                            # 기존 주문 취소
                            self.api.cancel_order(order_info['symbol'], order_id)
                            
                            # 시장가 청산 주문
                            market_order = self.api.place_order(
                                symbol=order_info['symbol'],
                                side=order_info['side'],
                                trade_side='close',
                                size=order_info['size'],
                                order_type='market'
                            )
                            
                            del self.pending_orders[order_id]
                            
                        # 진입 주문인 경우 (30초 후 취소)
                        elif not order_info['is_close'] and current_time - order_time > 30:
                            logger.info(f"Cancelling entry order: {order_id}")
                            self.api.cancel_order(order_info['symbol'], order_id)
                            del self.pending_orders[order_id]
                            
                            if order_info['symbol'] in self.positions:
                                self.update_position_status(order_info['symbol'], is_closed=True)
                
            except Exception as e:
                logger.error(f"Error checking pending orders: {e}")
                
            await asyncio.sleep(self.order_check_interval)
            
    async def open_position(self, symbol: str, side: str, size: float, 
                       leverage: int, stop_loss_price: float, 
                       take_profit_price: float, current_price: float,
                       order_type: str = 'limit', price: str = None) -> bool:
        """포지션 오픈"""
        try:
            # 기존 포지션 체크
            if symbol in self.positions:
                logger.warning(f"Position already exists for {symbol}")
                return False
                
            # 레버리지 설정
            await self.set_leverage(symbol, leverage)
            
            # API 파라미터 설정
            api_side = 'buy' if side == 'long' else 'sell'
            
            # Limit 주문 파라미터
            order_params = {
                'symbol': symbol,
                'side': api_side,
                'trade_side': 'open',
                'size': str(size),
                'order_type': order_type,
            }
            
            if order_type == 'limit' and price:
                order_params['price'] = price
            
            # 메인 오더 실행
            response = self.api.place_order(**order_params)
            
            if response.get('code') == '00000':
                order_id = response['data']['orderId']
                
                # 미체결 주문 정보 저장
                self.pending_orders[order_id] = {
                    'symbol': symbol,
                    'side': api_side,
                    'size': size,
                    'is_close': False,
                    'timestamp': time.time()
                }
                # 스탑로스 주문
                sl_response = self.api.place_order(
                    symbol=symbol,
                    side='sell' if side == 'long' else 'buy',
                    trade_side='close',
                    size=str(size),
                    order_type='stop',
                    trigger_price=str(stop_loss_price)
                )
                
                # 테이크프로핏 주문 (Limit)
                tp_response = self.api.place_order(
                    symbol=symbol,
                    side='sell' if side == 'long' else 'buy',
                    trade_side='close',
                    size=str(size),
                    order_type='limit',
                    price=str(take_profit_price)
                )
                
                # 포지션 기록
                self.positions[symbol] = Position(
                    symbol=symbol,
                    side=side,
                    size=size,
                    entry_price=float(price) if price else current_price,
                    stop_loss_price=stop_loss_price,
                    take_profit_price=take_profit_price,
                    timestamp=int(datetime.now().timestamp() * 1000),
                    leverage=leverage
                )
                
                return True
                
            return False
        
        except Exception as e:
            logger.error(f"Error opening position: {e}")
            return False
        
    async def get_position(self, symbol: str) -> Optional[Position]:
        """현재 포지션 상태 조회"""
        try:
            current_position = self.positions.get(symbol)
            if not current_position:
                return None
                
            # 포지션이 청산되었는지 확인하는 로직 추가
            if current_position.size == 0:
                del self.positions[symbol]
                return None
                
            return current_position
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None
            
    def update_position_status(self, symbol: str, is_closed: bool = False):
        """포지션 상태 업데이트"""
        if is_closed and symbol in self.positions:
            del self.positions[symbol]