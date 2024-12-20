# database_manager.py
import mysql.connector
import logging
import os
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None

class DatabaseManager:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not DatabaseManager._initialized:
            try:
                self.db = self._setup_database()
                DatabaseManager._initialized = True
            except Exception as e:
                logger.error(f"Failed to initialize DatabaseManager: {e}")
                raise
    
    def _setup_database(self):
        """데이터베이스 연결 및 테이블 설정"""
        try:
            db = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD')
            )
            
            cursor = db.cursor()
            db_name = os.getenv('MYSQL_DATABASE')
            
            # 기존 테이블 삭제 (스키마 변경을 위해)
            cursor.execute(f"""
                DROP TABLE IF EXISTS {db_name}.kline_1m
            """)
            
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")
            
            # quote_volume 컬럼 추가된 새로운 테이블 생성
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS kline_1m (
                timestamp BIGINT PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                quote_volume FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            db.commit()
            cursor.close()
            return db
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def reconnect(self):
        """DB 재연결"""
        try:
            if not self.db.is_connected():
                self.db = self._setup_database()
        except Exception as e:
            logger.error(f"Database reconnection error: {e}")
            raise

    def store_candle(self, candle: Candle):
        """단일 캔들 데이터 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                INSERT INTO kline_1m 
                (timestamp, open, high, low, close, volume, quote_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
            """, (
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume
            ))
            
            self.db.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing candle data: {e}")
            self.db.rollback()

    def get_recent_candles(self, limit: int = 200) -> List[Candle]:
        """최근 캔들 데이터 조회"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, quote_volume
                FROM kline_1m
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [
                Candle(
                    timestamp=row[0],
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    quote_volume=float(row[6]) if row[6] else None
                )
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error fetching recent candles: {e}")
            return []

    def store_initial_candles(self, candles: List[Dict]):
        """초기 캔들 데이터 일괄 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            for candle_data in candles:
                cursor.execute("""
                    INSERT INTO kline_1m 
                    (timestamp, open, high, low, close, volume, quote_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
                """, (
                    int(candle_data[0]),     # timestamp
                    float(candle_data[1]),   # open
                    float(candle_data[2]),   # high
                    float(candle_data[3]),   # low
                    float(candle_data[4]),   # close
                    float(candle_data[5]),   # volume
                    float(candle_data[6]),   # quote_volume
                    float(candle_data[1]),   # open
                    float(candle_data[2]),   # high
                    float(candle_data[3]),   # low
                    float(candle_data[4]),   # close
                    float(candle_data[5]),   # volume
                    float(candle_data[6])    # quote_volume
                ))
            
            self.db.commit()
            cursor.close()
            logger.info(f"Successfully stored {len(candles)} initial candles")
            
        except Exception as e:
            logger.error(f"Error storing initial candles: {e}")
            self.db.rollback()