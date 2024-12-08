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
        logger.info(f"Subscription request sent: {subscribe_data}")
            
        # Start processing messages
        asyncio.create_task(self._process_messages())
        
    async def _process_messages(self):
        logger.info("Started processing websocket messages")
        while True:
            try:
                message = await self.ws.recv()
                if message == 'pong':
                    continue
                    
                data = json.loads(message)
                logger.info(f"Full received data: {data}")
                
                # 데이터베이스 연결 확인 및 재연결
                try:
                    self.db.ping(reconnect=True)
                except Exception as e:
                    logger.error(f"Database connection error: {e}")
                    self.db = self._setup_database()
                
                if data.get('action') == 'update' and 'data' in data:
                    cursor = self.db.cursor()
                    
                    for candle_data in data['data']:
                        try:
                            if isinstance(candle_data, list) and len(candle_data) >= 6:
                                # 데이터 파싱
                                timestamp = int(candle_data[0])
                                open_price = float(candle_data[1])
                                high_price = float(candle_data[2])
                                low_price = float(candle_data[3])
                                close_price = float(candle_data[4])
                                volume = float(candle_data[5])
                                
                                logger.info(f"Attempting to insert candle: ts={timestamp}, o={open_price}, h={high_price}, l={low_price}, c={close_price}, v={volume}")
                                
                                # 쿼리 실행 전 테이블 존재 확인
                                cursor.execute("SHOW TABLES LIKE 'kline_1m'")
                                if not cursor.fetchone():
                                    logger.info("Table kline_1m not found, creating...")
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
                                    self.db.commit()
                                
                                # 데이터 삽입
                                cursor.execute("""
                                    INSERT INTO kline_1m (timestamp, open, high, low, close, volume)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                    open=VALUES(open), high=VALUES(high), low=VALUES(low),
                                    close=VALUES(close), volume=VALUES(volume)
                                """, (timestamp, open_price, high_price, low_price, close_price, volume))
                                
                                # 즉시 커밋
                                self.db.commit()
                                logger.info(f"Successfully inserted/updated candle with timestamp {timestamp}")
                                
                                # 데이터가 실제로 저장되었는지 확인
                                cursor.execute("SELECT * FROM kline_1m WHERE timestamp = %s", (timestamp,))
                                check_data = cursor.fetchone()
                                logger.info(f"Verification - Data in database: {check_data}")
                                
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
                            logger.error(f"Error processing individual candle: {e}, candle_data: {candle_data}")
                            continue
                            
            except Exception as e:
                if isinstance(e, websockets.exceptions.ConnectionClosedOK):
                    logger.info("WebSocket connection closed normally")
                else:
                    logger.error(f"Error processing message: {e}")
                await asyncio.sleep(1)
                continue