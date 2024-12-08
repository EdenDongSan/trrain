import time
import base64
import hmac
import hashlib
import requests
from urllib.parse import urlencode
import logging

logger = logging.getLogger(__name__)

class BitgetAPI:
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.API_KEY = api_key
        self.SECRET_KEY = secret_key
        self.PASSPHRASE = passphrase
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
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, request_path, body)
        
        return {
            "ACCESS-KEY": self.API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.PASSPHRASE,
            "Content-Type": "application/json",
            "ACCESS-VERSION": "2"
        }
    def get_account_balance(self, symbol: str = 'BTCUSDT', margin_coin: str = 'USDT'):
        """계좌 잔고 조회"""
        endpoint = "/api/v2/mix/account/account"
        method = "GET"
        
        request_path = f"{endpoint}?symbol={symbol}&marginCoin={margin_coin}"
        headers = self._create_headers(method, request_path)
        
        response = requests.get(
            self.BASE_URL + request_path,
            headers=headers
        )
        
        return response.json()
    
    def place_order(self, symbol: str, side: str, trade_side: str, size: str, 
               margin_coin: str = 'USDT', order_type: str = 'market'):
        """
        Place a futures order
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            side: 'buy' or 'sell'
            trade_side: 'open' or 'close'
            size: Order quantity
            margin_coin: Margin coin, default 'USDT'
            order_type: Order type, default 'market'
        
        Examples:
            Long position open: side='buy', trade_side='open'
            Long position close: side='sell', trade_side='close'
            Short position open: side='sell', trade_side='open'
            Short position close: side='buy', trade_side='close'
        """
        endpoint = "/api/v2/mix/order/place-order"
        method = "POST"
        
        body = {
            "symbol": symbol,
            "marginCoin": margin_coin,
            "side": side,
            "tradeSide": trade_side,
            "size": size,
            "orderType": order_type,
        }
        
        headers = self._create_headers(method, endpoint, str(body))
        response = requests.post(
            self.BASE_URL + endpoint, 
            headers=headers,
            json=body
        )
        
        return response.json()
        
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