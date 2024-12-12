import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime
import asyncio
from data_api import BitgetAPI
import time
from models import Position

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
        
    async def wait_for_order_fill(self, symbol: str, order_id: str, timeout: int = 30) -> bool:
        """주문 체결 대기"""
        start_time = time.time()
        logger.info(f"Waiting for order {order_id} to fill (timeout: {timeout}s)")
        
        while time.time() - start_time < timeout:
            try:
                response = self.api.get_order_detail(symbol, order_id)
                
                if response.get('code') == '00000':
                    status = response['data']['state']
                    price = response['data'].get('priceAvg', '0')
                    logger.info(f"Order {order_id} status: {status}, avg price: {price}")
                    
                    if status == 'filled':
                        logger.info(f"Order {order_id} filled successfully")
                        return True
                    elif status in ['cancelled', 'canceled']:
                        logger.warning(f"Order {order_id} was cancelled")
                        return False
                        
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error checking order status: {e}")
                await asyncio.sleep(1)
                
        logger.warning(f"Order {order_id} fill timeout after {timeout}s")
        return False
        
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """레버리지 설정"""
        try:
            # Bitget API에 레버리지 설정하는 코드 추가 필요
            return True
        except Exception as e:
            logger.error(f"Error setting leverage: {e}")
            return False
        
    async def check_pending_orders(self):
        """미체결 주문 상태 확인 및 관리"""
        while True:
            try:
                # API를 통해 실제 미체결 주문 조회
                response = self.api.get_pending_orders("BTCUSDT")
                if response.get('code') != '00000':
                    logger.error(f"Failed to get pending orders: {response}")
                    await asyncio.sleep(self.order_check_interval)
                    continue

                current_pending = {
                    order['orderId']: order 
                    for order in response.get('data', {}).get('entrustedList', [])
                }
                
                # 현재 시간
                current_time = time.time()
                
                # 기존 pending_orders와 비교하여 관리
                for order_id, order_info in list(self.pending_orders.items()):
                    # API에서 조회된 미체결 주문에 없는 경우 (이미 체결됨)
                    if order_id not in current_pending:
                        logger.info(f"Order {order_id} is no longer pending")
                        del self.pending_orders[order_id]
                        continue
                        
                    api_order = current_pending[order_id]
                    order_time = float(order_info['timestamp'])
                    
                    # 청산 주문인 경우 (20초 후 시장가 전환)
                    if order_info['is_close'] and current_time - order_time > 20:
                        logger.info(f"Converting close order to market order: {order_id}")
                        
                        # 기존 주문 취소
                        cancel_response = self.api.cancel_order(order_info['symbol'], order_id)
                        if cancel_response.get('code') == '00000':
                            # 시장가 청산 주문
                            market_order = self.api.place_order(
                                symbol=order_info['symbol'],
                                side=order_info['side'],
                                trade_side='close',
                                size=order_info['size'],
                                order_type='market'
                            )
                            logger.info(f"Market close order placed: {market_order}")
                        
                        del self.pending_orders[order_id]
                        
                    # 진입 주문인 경우 (30초 후 취소)
                    elif not order_info['is_close'] and current_time - order_time > 30:
                        logger.info(f"Cancelling entry order: {order_id}")
                        cancel_response = self.api.cancel_order(order_info['symbol'], order_id)
                        
                        if cancel_response.get('code') == '00000':
                            logger.info(f"Successfully cancelled entry order: {order_id}")
                            if order_info['symbol'] in self.positions:
                                self.update_position_status(order_info['symbol'], is_closed=True)
                        else:
                            logger.error(f"Failed to cancel entry order: {cancel_response}")
                            
                        del self.pending_orders[order_id]
                
                # 새로운 미체결 주문 추가
                for order_id, api_order in current_pending.items():
                    if order_id not in self.pending_orders:
                        self.pending_orders[order_id] = {
                            'symbol': api_order['symbol'],
                            'side': api_order['side'],
                            'size': api_order['size'],
                            'timestamp': float(api_order['cTime']) / 1000,  # ms를 s로 변환
                            'is_close': api_order['tradeSide'] == 'close',
                            'order_type': api_order['orderType']
                        }
                        logger.info(f"Added new pending order: {order_id}")
                    
            except Exception as e:
                logger.error(f"Error in check_pending_orders: {e}")
                
            await asyncio.sleep(self.order_check_interval)

    async def cancel_all_symbol_orders(self, symbol: str) -> bool:
        """특정 심볼의 모든 미체결 주문 취소"""
        try:
            cancel_results = self.api.cancel_all_pending_orders(symbol)
            success = all(result.get('code') == '00000' for result in cancel_results)
            
            if success:
                # pending_orders에서 해당 심볼의 주문 제거
                self.pending_orders = {
                    order_id: info 
                    for order_id, info in self.pending_orders.items() 
                    if info['symbol'] != symbol
                }
                logger.info(f"Successfully cancelled all pending orders for {symbol}")
            else:
                logger.error(f"Some orders failed to cancel for {symbol}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error cancelling all orders for {symbol}: {e}")
            return False
                
    async def open_position(self, symbol: str, side: str, size: float, 
                       leverage: int, stop_loss_price: float, 
                       take_profit_price: float, current_price: float,
                       order_type: str = 'limit', price: str = None) -> bool:
        """포지션 오픈"""
        try:
            position = self.api.get_position(symbol)
            if position:
                logger.warning(f"Position already exists for {symbol}")
                return False
                
            # 레버리지 설정
            await self.set_leverage(symbol, leverage)
            
            # API 파라미터 설정
            api_side = 'buy' if side == 'long' else 'sell'
            hold_side = side  # long 또는 short
            
            # 메인 오더 실행
            order_params = {
                'symbol': symbol,
                'side': api_side,
                'trade_side': 'open',
                'size': str(size),
                'order_type': order_type,
            }
            
            if order_type == 'limit' and price:
                order_params['price'] = price
            
            response = self.api.place_order(**order_params)
            
            if response.get('code') == '00000':
                order_id = response['data']['orderId']
                logger.info(f"Main order placed successfully: {order_id}")
                
                # 리밋 주문인 경우 체결 대기
                if order_type == 'limit':
                    order_filled = await self.wait_for_order_fill(symbol, order_id)
                    if not order_filled:
                        logger.error("Order not filled within timeout")
                        self.api.cancel_order(symbol, order_id)
                        return False
                else:
                    # 시장가 주문인 경우 짧은 대기
                    await asyncio.sleep(2)
                
                # 포지션 생성 확인
                position = None
                retry_count = 0
                while retry_count < 5:  # 최대 5회 확인
                    position = self.api.get_position(symbol)
                    if position:
                        break
                    retry_count += 1
                    await asyncio.sleep(1)
                
                if not position:
                    logger.error("Position was not created after order fill")
                    return False
                
                # TP/SL 설정 재시도 로직
                sl_set = False
                tp_set = False
                
                # 스탑로스 주문 (최대 3회 시도)
                for attempt in range(3):
                    logger.info(f"Setting stop loss, attempt {attempt + 1}/3")
                    sl_response = self.api.place_tpsl_order(
                        symbol=symbol,
                        plan_type='loss_plan',
                        trigger_price=str(stop_loss_price),
                        hold_side=hold_side,
                        size=str(size)
                    )
                    
                    if sl_response.get('code') == '00000':
                        sl_set = True
                        logger.info("Stop loss set successfully")
                        break
                    
                    logger.error(f"Failed to set stop loss: {sl_response}")
                    await asyncio.sleep(1)
                
                # 테이크프로핏 주문 (최대 3회 시도)
                for attempt in range(3):
                    logger.info(f"Setting take profit, attempt {attempt + 1}/3")
                    tp_response = self.api.place_tpsl_order(
                        symbol=symbol,
                        plan_type='profit_plan',
                        trigger_price=str(take_profit_price),
                        hold_side=hold_side,
                        size=str(size)
                    )
                    
                    if tp_response.get('code') == '00000':
                        tp_set = True
                        logger.info("Take profit set successfully")
                        break
                    
                    logger.error(f"Failed to set take profit: {tp_response}")
                    await asyncio.sleep(1)
                
                # TP/SL 설정 실패 시 포지션 청산
                if not (sl_set and tp_set):
                    logger.error("Failed to set TP/SL orders, closing position")
                    self.api.close_position(symbol)
                    return False
                
                # 모든 주문이 성공적으로 실행된 경우
                self.positions[symbol] = Position(
                    symbol=symbol,
                    side=side,
                    size=float(size),
                    entry_price=float(price) if price else current_price,
                    stop_loss_price=stop_loss_price,
                    take_profit_price=take_profit_price,
                    timestamp=int(time.time() * 1000),
                    leverage=leverage
                )
                
                logger.info(f"Successfully opened position with TP/SL orders for {symbol}")
                return True
            
            logger.error(f"Failed to place main order: {response}")
            return False
        
        except Exception as e:
            logger.error(f"Error opening position: {e}")
            try:
                self.api.close_position(symbol)
            except Exception as close_error:
                logger.error(f"Error closing position after error: {close_error}")
            return False
        
    def get_position(self, symbol: str) -> Optional[Position]:
        """현재 포지션 상태 조회"""
        try:
            return self.api.get_position(symbol)
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None
            
    def update_position_status(self, symbol: str, is_closed: bool = False):
        """포지션 상태 업데이트"""
        if is_closed and symbol in self.positions:
            del self.positions[symbol]