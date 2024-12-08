import mysql.connector
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

class MarketDataManager:
    def __init__(self):
        self.db = self._connect_db()
        self.latest_candle: Optional[Candle] = None
        self.candles_cache: Dict[int, Candle] = {}  # timestamp: Candle
        self._initialize_cache()

    def _connect_db(self) -> mysql.connector.MySQLConnection:
        """데이터베이스 연결"""
        return mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )

    def _initialize_cache(self, lookback_minutes: int = 100) -> None:
        """초기 캐시 구성 - 최근 N분의 데이터를 메모리에 로드"""
        try:
            cursor = self.db.cursor(dictionary=True)
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM kline_1m
                WHERE timestamp > %s
                ORDER BY timestamp DESC
            """
            lookback_ms = int((datetime.now() - timedelta(minutes=lookback_minutes)).timestamp() * 1000)
            
            cursor.execute(query, (lookback_ms,))
            rows = cursor.fetchall()
            
            for row in rows:
                candle = Candle(
                    timestamp=row['timestamp'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume']
                )
                self.candles_cache[row['timestamp']] = candle
                
            if rows:
                self.latest_candle = Candle(**rows[0])
                
            logger.info(f"Initialized cache with {len(rows)} candles")
            
        except Exception as e:
            logger.error(f"Error initializing cache: {e}")
        finally:
            cursor.close()

    def update_latest_candle(self, candle: Candle) -> None:
        """새로운 캔들 데이터로 캐시 업데이트"""
        self.latest_candle = candle
        self.candles_cache[candle.timestamp] = candle
        
        if len(self.candles_cache) > 200:
            oldest_timestamp = min(self.candles_cache.keys())
            del self.candles_cache[oldest_timestamp]

    def get_latest_price(self) -> float:
        """현재 가격 조회"""
        return self.latest_candle.close if self.latest_candle else 0.0

    def get_recent_candles(self, lookback: int = 20) -> List[Candle]:
        """최근 N개의 캔들 데이터 조회"""
        sorted_timestamps = sorted(self.candles_cache.keys(), reverse=True)
        return [self.candles_cache[ts] for ts in sorted_timestamps[:lookback]]

    def get_price_data_as_df(self, lookback: int = 100) -> pd.DataFrame:
        """최근 N개의 캔들 데이터를 pandas DataFrame으로 반환"""
        candles = self.get_recent_candles(lookback)
        data = {
            'timestamp': [c.timestamp for c in candles],
            'open': [c.open for c in candles],
            'high': [c.high for c in candles],
            'low': [c.low for c in candles],
            'close': [c.close for c in candles],
            'volume': [c.volume for c in candles]
        }
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.sort_values('timestamp')

    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        """EMA 계산"""
        return df['close'].ewm(span=period, adjust=False).mean()

    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def calculate_stoch_rsi(self, 
                          period: int = 14, 
                          smoothk: int = 3, 
                          smoothd: int = 3) -> Tuple[float, float]:
        """
        Stochastic RSI 계산
        Returns:
            Tuple[float, float]: (K값, D값)
        """
        try:
            # 충분한 데이터 확보를 위해 더 긴 기간의 데이터 가져오기
            df = self.get_price_data_as_df(lookback=period*2)
            
            # RSI 계산
            rsi = self.calculate_rsi(df, period)
            
            # Stochastic RSI 계산
            rsi_min = rsi.rolling(window=period).min()
            rsi_max = rsi.rolling(window=period).max()
            stoch_rsi = 100 * (rsi - rsi_min) / (rsi_max - rsi_min)
            
            # K값 (Fast Stochastic)
            k = stoch_rsi.rolling(window=smoothk).mean()
            
            # D값 (Slow Stochastic)
            d = k.rolling(window=smoothd).mean()
            
            # 최신 값 반환
            return k.iloc[-1], d.iloc[-1]
            
        except Exception as e:
            logger.error(f"Error calculating Stochastic RSI: {e}")
            return 0.0, 0.0

    def calculate_technical_indicators(self, lookback: int = 100) -> Dict[str, float]:
        """기술적 지표 계산"""
        df = self.get_price_data_as_df(lookback)
        
        if len(df) < lookback:
            return {}

        # EMA 계산 (7, 25, 99)
        ema7 = self.calculate_ema(df, 7).iloc[-1]
        ema25 = self.calculate_ema(df, 25).iloc[-1]
        ema200 = self.calculate_ema(df, 200).iloc[-1]
       
        # 가격 변화 계산 (이전 봉 대비)
        price_change = df['close'].diff().iloc[-1]

        # Stochastic RSI 계산
        stoch_k, stoch_d = self.calculate_stoch_rsi()

        result = {
            'ema7': ema7,
            'ema25': ema25,
            'ema200': ema200,
            'price_change': price_change,
            'stoch_k': stoch_k,
            'stoch_d': stoch_d,
            'last_close': df['close'].iloc[-1],
            'last_volume': df['volume'].iloc[-1],
        }
        
        return result
        
    async def handle_websocket_update(self, data: Dict) -> None:
        """웹소켓으로부터 새로운 캔들 데이터를 받았을 때 처리"""
        try:
            candle = Candle(
                timestamp=data['timestamp'],
                open=float(data['open']),
                high=float(data['high']),
                low=float(data['low']),
                close=float(data['close']),
                volume=float(data['volume'])
            )
            self.update_latest_candle(candle)
            
            # 최신 기술적 지표 계산
            indicators = self.calculate_technical_indicators()
            logger.info(f"Updated indicators: {indicators}")
            
        except Exception as e:
            logger.error(f"Error handling websocket update: {e}")