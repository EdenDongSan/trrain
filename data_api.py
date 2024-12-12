import time
import base64
import hmac
import hashlib
import requests
from urllib.parse import urlencode
import logging
from logging_setup import APILogger
import json
from models import Position
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class BitgetAPI:
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.API_KEY = api_key
        self.SECRET_KEY = secret_key
        self.PASSPHRASE = passphrase
        self.api_logger = APILogger()
        self.BASE_URL = "https://api.bitget.com"
        
    def _generate_signature(self, timestamp: str, method: str, 
                          request_path: str, body: str = '') -> str:
        message = timestamp + method + request_path + body
        mac = hmac.new(
            bytes(self.SECRET_KEY, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        d = mac.digest()
        return base64.b64encode(d).decode()
    
    def _create_headers(self, method: str, request_path: str, body: str = '') -> dict:
        """API 요청 헤더 생성"""
        timestamp = str(int(time.time() * 1000))
        
        # 쿼리 파라미터가 있는 경우 정렬
        if '?' in request_path:
            base_path, query = request_path.split('?', 1)
            # 쿼리 파라미터를 키로 정렬
            params = sorted(query.split('&'))
            request_path = base_path + '?' + '&'.join(params)
        
        # 서명할 메시지 생성
        message = timestamp + method.upper() + request_path + body
        
        # HMAC SHA256 서명 생성
        mac = hmac.new(
            bytes(self.SECRET_KEY, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        signature = base64.b64encode(mac.digest()).decode()
        
        logger.info(f"Timestamp: {timestamp}")
        logger.info(f"Method: {method.upper()}")
        logger.info(f"Request path: {request_path}")
        logger.info(f"Body: {body}")
        logger.info(f"Message to sign: {message}")
        
        return {
            "ACCESS-KEY": self.API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.PASSPHRASE,
            "Content-Type": "application/json",
            "ACCESS-VERSION": "2"
        }
    
    def get_kline_history(self, symbol: str = 'BTCUSDT', granularity: str = '1m', limit: int = 50):
        """
        과거 캔들스틱 데이터 조회
        """
        endpoint = "/api/v2/mix/market/candles"
        method = "GET"
        
        # 쿼리 파라미터 (알파벳 순으로 정렬)
        query = (f"granularity={granularity}&"
                f"limit={limit}&"
                f"productType=USDT-FUTURES&"
                f"symbol={symbol}")
        
        request_path = f"{endpoint}?{query}"
        headers = self._create_headers(method, request_path)
        full_url = self.BASE_URL + request_path
        
        try:
            response = requests.get(
                full_url,
                headers=headers
            )
            
            logger.info(f"Requesting historical candles: {full_url}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '00000':
                    candles = result.get('data', [])
                    logger.info(f"Received {len(candles)} historical candles")
                    return result
            else:
                error_msg = f"Failed to get historical candles: {response.text}"
                logger.error(error_msg)
                return None
                
        except Exception as e:
            logger.error(f"Error fetching kline history: {e}")
            return None

    def get_account_balance(self):
            """계좌 잔고 조회"""
            endpoint = "/api/v2/mix/account/accounts"
            method = "GET"
            query = "productType=USDT-FUTURES"
            request_path = f"{endpoint}?{query}"
            
            headers = self._create_headers(method, request_path)
            full_url = self.BASE_URL + request_path
            
            try:
                response = requests.get(
                    full_url,
                    headers=headers
                )
                
                logger.info(f"Full URL: {full_url}")
                logger.info(f"Response status: {response.status_code}")
                logger.info(f"Response text: {response.text}")
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('code') == '00000' and result.get('data'):
                        return {
                            'code': '00000',
                            'data': result['data'][0]
                        }
                    return result
                else:
                    error_msg = f"API request failed with status {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    return {"code": "error", "msg": error_msg, "data": None}
                    
            except Exception as e:
                logger.error(f"Error in get_account_balance: {e}")
                return {"code": "error", "msg": str(e), "data": None}
            
        
    def place_order(self, symbol: str, side: str, trade_side: str, size: str, 
                margin_coin: str = 'USDT', order_type: str = 'limit', 
                price: str = None, trigger_price: str = None):
        """주문 생성"""
        endpoint = "/api/v2/mix/order/place-order"
        method = "POST"
        
        # 가격을 0.1의 배수로 반올림
        if price:
            price = str(round(float(price) * 10) / 10)
        if trigger_price:
            trigger_price = str(round(float(trigger_price) * 10) / 10)
        
        # 주문 타입 매핑
        order_type_mapping = {
            'market': 'market',
            'limit': 'limit',
            'stop': 'profit_stop'  # 스탑 주문 타입 수정
        }
        
        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": margin_coin,
            "side": side,
            "tradeSide": trade_side,
            "orderType": order_type_mapping.get(order_type, 'limit'),
            "size": size,
        }
        
        if order_type == 'limit' and price:
            body["price"] = price
        elif order_type == 'stop' and trigger_price:
            body["triggerPrice"] = trigger_price
            body["holdSide"] = "short" if side == "buy" else "long"
        
        logger.info(f"Placing order with params: {body}")
        
        try:
            body_str = json.dumps(body)
            headers = self._create_headers(method, endpoint, body_str)
            response = requests.post(
                self.BASE_URL + endpoint,
                headers=headers,
                json=body
            )
            
            logger.info(f"Order response: {response.text}")
            return response.json()
            
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {"code": "error", "msg": str(e)}
        
    def place_tpsl_order(self, symbol: str, plan_type: str, trigger_price: str,
                    hold_side: str, size: str, execute_price: str = "0"):
        """
        스탑로스/테이크프로핏 주문 생성
        
        Args:
            symbol: 거래 심볼 (예: 'BTCUSDT')
            plan_type: 'profit_plan' 또는 'loss_plan'
            trigger_price: 트리거 가격
            hold_side: 'long' 또는 'short'
            size: 주문 수량
            execute_price: 실행 가격 (0이면 시장가)
        """
        endpoint = "/api/v2/mix/order/place-tpsl-order"
        method = "POST"
        
        # 가격을 0.1의 배수로 반올림
        trigger_price = str(round(float(trigger_price) * 10) / 10)
        if execute_price != "0":
            execute_price = str(round(float(execute_price) * 10) / 10)
        
        body = {
            "symbol": symbol.upper(),  # API는 대문자 심볼을 요구
            "marginCoin": "USDT",
            "productType": "USDT-FUTURES",  # 필수 파라미터
            "planType": plan_type,
            "triggerPrice": trigger_price,
            "triggerType": "mark_price",  # 안정적인 마크 가격 사용
            "executePrice": execute_price,
            "holdSide": hold_side,  # long 또는 short
            "size": size
        }
        
        try:
            body_str = json.dumps(body)
            headers = self._create_headers(method, endpoint, body_str)
            
            logger.info(f"Placing TPSL order with params: {body}")
            
            response = requests.post(
                self.BASE_URL + endpoint,
                headers=headers,
                json=body
            )
            
            logger.info(f"TPSL order response: {response.text}")
            return response.json()
            
        except Exception as e:
            logger.error(f"Error placing TPSL order: {e}")
            return {"code": "error", "msg": str(e)}
                
    def close_position(self, symbol: str, margin_coin: str = 'USDT'):
        """Close all positions for a symbol"""
        endpoint = "/api/v2/mix/order/close-positions"
        method = "POST"
        
        body = {
            "symbol": symbol,
            "marginCoin": margin_coin,
        }
        
        headers = self._create_headers(method, endpoint, str(body))
        response = requests.post(
            self.BASE_URL + endpoint,
            headers=headers,
            json=body
        )
        
        return response.json()
    
    def get_order_detail(self, symbol: str, order_id: str):
        """주문 상태 조회"""
        endpoint = f"/api/v2/mix/order/detail"
        method = "GET"
        
        request_path = f"{endpoint}?symbol={symbol}&orderId={order_id}"
        headers = self._create_headers(method, request_path)
        
        response = requests.get(
            self.BASE_URL + request_path,
            headers=headers
        )
        return response.json()
    
    def cancel_order(self, symbol: str, order_id: str):
        """주문 취소"""
        endpoint = "/api/v2/mix/order/cancel-order"
        method = "POST"
        
        body = {
            "symbol": symbol,
            "orderId": order_id
        }
        
        headers = self._create_headers(method, endpoint, str(body))
        response = requests.post(
            self.BASE_URL + endpoint,
            headers=headers,
            json=body
        )
        return response.json()
    
    # data_api.py의 BitgetAPI 클래스 내부
    def get_position(self, symbol: str) -> Optional[Position]:
        """포지션 정보 조회"""
        endpoint = "/api/v2/mix/position/single-position"
        method = "GET"
        
        params = {
            'symbol': symbol,
            'marginMode': 'crossed',  # crossed로 통일
            'productType': 'USDT-FUTURES',
            'marginCoin': 'USDT'
        }
        
        query = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
        request_path = f"{endpoint}?{query}"
        
        headers = self._create_headers(method, request_path)
        full_url = self.BASE_URL + request_path
        
        try:
            logger.info(f"Requesting position info for {symbol}")
            response = requests.get(
                full_url,
                headers=headers
            )
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response text: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '00000' and result.get('data'):
                    position_data = result['data'][0] if isinstance(result['data'], list) else result['data']
                    logger.info(f"Position data: {position_data}")
                    
                    # 포지션이 있는 경우
                    if float(position_data.get('total', '0')) > 0:
                        entry_price = float(position_data.get('openPriceAvg', '0'))
                        
                        position = Position(
                            symbol=symbol,
                            side='long' if position_data.get('holdSide') == 'long' else 'short',
                            size=float(position_data.get('total', '0')),
                            entry_price=entry_price,
                            stop_loss_price=0.0,
                            take_profit_price=0.0,
                            timestamp=int(time.time() * 1000),
                            leverage=int(position_data.get('leverage', '1'))
                        )
                        
                        logger.info(f"Found position: {position}")
                        return position
                
                logger.info("No position found")
                return None
                    
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None
        
    def get_pending_orders(self, symbol: str = None, 
                         status: str = None, limit: str = "100") -> dict:
        """
        미체결 주문 조회
        
        Args:
            symbol: 거래 쌍 (예: 'BTCUSDT')
            status: 주문 상태 ('live' 또는 'partially_filled')
            limit: 조회할 주문 수 (최대 100)
            
        Returns:
            dict: API 응답 데이터
        """
        endpoint = "/api/v2/mix/order/orders-pending"
        method = "GET"
        
        # 쿼리 파라미터 구성
        params = {
            'productType': 'USDT-FUTURES',
            'limit': limit
        }
        
        if symbol:
            params['symbol'] = symbol
        if status:
            params['status'] = status
            
        # 파라미터 정렬 및 쿼리 스트링 생성
        query = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
        request_path = f"{endpoint}?{query}"
        
        headers = self._create_headers(method, request_path)
        full_url = self.BASE_URL + request_path
        
        try:
            logger.info(f"Requesting pending orders: {full_url}")
            response = requests.get(
                full_url,
                headers=headers
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '00000':
                    orders = result.get('data', {}).get('entrustedList', [])
                    logger.info(f"Retrieved {len(orders)} pending orders")
                    return result
                else:
                    logger.error(f"API error: {result}")
            else:
                logger.error(f"HTTP error {response.status_code}: {response.text}")
            
            return {"code": "error", "msg": response.text}
            
        except Exception as e:
            logger.error(f"Error getting pending orders: {e}")
            return {"code": "error", "msg": str(e)}
            
    def cancel_all_pending_orders(self, symbol: str) -> List[dict]:
        """
        모든 미체결 주문 취소
        
        Args:
            symbol: 거래 쌍 (예: 'BTCUSDT')
            
        Returns:
            List[dict]: 취소된 주문들의 응답 목록
        """
        try:
            # 미체결 주문 조회
            pending_orders = self.get_pending_orders(symbol)
            if pending_orders.get('code') != '00000':
                logger.error(f"Failed to get pending orders: {pending_orders}")
                return []
                
            cancel_results = []
            orders = pending_orders.get('data', {}).get('entrustedList', [])
            
            for order in orders:
                order_id = order.get('orderId')
                if not order_id:
                    continue
                    
                # 주문 취소
                cancel_response = self.cancel_order(symbol, order_id)
                cancel_results.append(cancel_response)
                
                if cancel_response.get('code') == '00000':
                    logger.info(f"Successfully cancelled order {order_id}")
                else:
                    logger.error(f"Failed to cancel order {order_id}: {cancel_response}")
                    
            return cancel_results
            
        except Exception as e:
            logger.error(f"Error cancelling all pending orders: {e}")
            return []