import asyncio
import websockets
import json
import logging
import mysql.connector
import os
from datetime import datetime
from market_data_manager import Candle

logger = logging.getLogger(__name__)

class BitgetWebsocket:
    def __init__(self, market_data_manager=None):
        self.WS_URL = "wss://ws.bitget.com/v2/ws/public"
        self.ws = None
        self.db = self._setup_database()
        self.market_data_manager = market_data_manager
        self.connected = False
        self.reconnecting = False
        self.subscriptions = []
        self._processing = False  # 메시지 처리 중인지 확인하는 플래그 추가

    def _setup_database(self):
        """데이터베이스 연결 설정"""
        try:
            db = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD')
            )
            
            cursor = db.cursor()
            db_name = os.getenv('MYSQL_DATABASE')
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kline_1m (
                    timestamp BIGINT PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.commit()
            return db
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    async def connect(self):
        """WebSocket 연결 설정 (재시도 로직 포함)"""
        while not self.connected and not self.reconnecting:
            try:
                self.reconnecting = True
                logger.info("Attempting to connect to WebSocket...")
                self.ws = await websockets.connect(self.WS_URL)
                self.connected = True
                self.reconnecting = False
                logger.info("WebSocket connected successfully")
                
                # 기존 구독 복구
                for symbol in self.subscriptions:
                    await self.subscribe_kline(symbol)
                    
                asyncio.create_task(self._keep_alive())
                return True
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                await asyncio.sleep(5)  # 5초 후 재시도
            finally:
                self.reconnecting = False
        return False

    async def disconnect(self):
        """WebSocket 연결 종료"""
        if self.ws:
            try:
                await self.ws.close()
                self.connected = False
                logger.info("WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")

    async def is_connected(self):
        """웹소켓 연결 상태 확인"""
        return (self.ws is not None and 
                hasattr(self.ws, 'state') and 
                self.ws.state == websockets.protocol.State.OPEN)

    async def _keep_alive(self):
        """연결 유지 및 재연결 관리"""
        while True:
            try:
                if await self.is_connected():
                    await self.ws.send('ping')
                    await asyncio.sleep(20)
                else:
                    logger.info("WebSocket disconnected, attempting reconnect...")
                    self.connected = False
                    await self.connect()
            except Exception as e:
                logger.error(f"Error in keep_alive: {e}")
                self.connected = False
                await asyncio.sleep(5)
                await self.connect()

    async def subscribe_kline(self, symbol: str = 'BTCUSDT'):
        """K라인 데이터 구독"""
        if symbol not in self.subscriptions:
            self.subscriptions.append(symbol)

        subscribe_data = {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": symbol
            }]
        }
        
        try:
            if await self.is_connected():
                await self.ws.send(json.dumps(subscribe_data))
                logger.info(f"Subscription request sent: {subscribe_data}")
                # process_messages가 이미 실행 중이 아닐 때만 새로 시작
                if not self._processing:
                    asyncio.create_task(self._process_messages())
            else:
                logger.warning("WebSocket not connected, attempting reconnection...")
                await self.connect()
        except Exception as e:
            logger.error(f"Error subscribing to kline: {e}")
            await self.connect()

    async def _process_messages(self):
        """메시지 처리"""
        if self._processing:
            return
            
        self._processing = True
        try:
            while await self.is_connected():
                try:
                    message = await self.ws.recv()
                    if message == 'pong':
                        continue
                        
                    data = json.loads(message)
                    logger.debug(f"Received data: {data}")
                    
                    # DB 연결 확인 및 재연결
                    try:
                        self.db.ping(reconnect=True)
                    except Exception as e:
                        logger.error(f"Database connection error: {e}")
                        self.db = self._setup_database()
                    
                    if data.get('action') == 'update' and 'data' in data:
                        await self._handle_kline_data(data['data'])

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed, attempting reconnect...")
                    self.connected = False
                    await asyncio.sleep(5)
                    await self.connect()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await asyncio.sleep(1)
                    
        finally:
            self._processing = False
    async def _handle_kline_data(self, candle_data_list):
        """K라인 데이터 처리"""
        cursor = self.db.cursor()
        
        for candle_data in candle_data_list:
            try:
                if isinstance(candle_data, list) and len(candle_data) >= 6:
                    timestamp = int(candle_data[0])
                    open_price = float(candle_data[1])
                    high_price = float(candle_data[2])
                    low_price = float(candle_data[3])
                    close_price = float(candle_data[4])
                    volume = float(candle_data[5])
                    
                    # DB에 저장
                    cursor.execute("""
                        INSERT INTO kline_1m (timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=VALUES(open), high=VALUES(high), low=VALUES(low),
                        close=VALUES(close), volume=VALUES(volume)
                    """, (timestamp, open_price, high_price, low_price, close_price, volume))
                    
                    self.db.commit()
                    
                    if self.market_data_manager:
                        candle_obj = Candle(
                            timestamp=timestamp,
                            open=open_price,
                            high=high_price,
                            low=low_price,
                            close=close_price,
                            volume=volume
                        )
                        self.market_data_manager.update_latest_candle(candle_obj)
                        
            except Exception as e:
                logger.error(f"Error processing candle data: {e}, data: {candle_data}")
                continue