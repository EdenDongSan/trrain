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
        self.market_data_manager = market_data_manager  # MarketDataManager 인스턴스 추가
        
    def _setup_database(self):
        """Setup MySQL database connection"""
        try:
            # 먼저 데이터베이스 없이 연결
            db = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD')
            )
            
            cursor = db.cursor()
            
            # 데이터베이스 생성
            db_name = os.getenv('MYSQL_DATABASE')
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            
            # 데이터베이스 선택
            cursor.execute(f"USE {db_name}")
            
            # 테이블 생성
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
        """Connect to websocket"""
        self.ws = await websockets.connect(self.WS_URL)
        asyncio.create_task(self._keep_alive())
        
    async def disconnect(self):
        """Disconnect from websocket"""
        if self.ws:
            await self.ws.close()
            
    async def _keep_alive(self):
        """Keep the websocket connection alive"""
        while True:
            if self.ws:
                await self.ws.send('ping')
                await asyncio.sleep(20)
                
    async def subscribe_kline(self, symbol: str = 'BTCUSDT'):
        """Subscribe to 1-minute kline data"""
        subscribe_data = {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": symbol
            }]
        }
        
        await self.ws.send(json.dumps(subscribe_data))
        logger.info(f"Subscribed to kline data for {symbol}")
            
        # Start processing messages
        asyncio.create_task(self._process_messages())
        
    async def _process_messages(self):
        while True:
            try:
                message = await self.ws.recv()
                if message == 'pong':
                    continue
                    
                data = json.loads(message)
                
                if 'data' in data:
                    cursor = self.db.cursor()
                    for candle in data['data']:
                        # DB 저장 로직
                        if isinstance(candle, list) and len(candle) >= 6:
                            cursor.execute("""
                                INSERT INTO kline_1m (timestamp, open, high, low, close, volume)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                                close=VALUES(close), volume=VALUES(volume)
                            """, candle[:6])
                            
                            # MarketDataManager 업데이트
                            if self.market_data_manager:
                                candle_obj = Candle(
                                    timestamp=candle[0],
                                    open=float(candle[1]),
                                    high=float(candle[2]),
                                    low=float(candle[3]),
                                    close=float(candle[4]),
                                    volume=float(candle[5])
                                )
                                self.market_data_manager.update_latest_candle(candle_obj)
                                
                        elif isinstance(candle, dict):
                            cursor.execute("""
                                INSERT INTO kline_1m (timestamp, open, high, low, close, volume)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                                close=VALUES(close), volume=VALUES(volume)
                            """, (
                                candle.get('timestamp'),
                                candle.get('open'),
                                candle.get('high'),
                                candle.get('low'),
                                candle.get('close'),
                                candle.get('volume')
                            ))
                            
                            # MarketDataManager 업데이트
                            if self.market_data_manager:
                                candle_obj = Candle(
                                    timestamp=candle.get('timestamp'),
                                    open=float(candle.get('open')),
                                    high=float(candle.get('high')),
                                    low=float(candle.get('low')),
                                    close=float(candle.get('close')),
                                    volume=float(candle.get('volume'))
                                )
                                self.market_data_manager.update_latest_candle(candle_obj)
                                
                    self.db.commit()
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue