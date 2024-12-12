import mysql.connector
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os
from data_api import BitgetAPI

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
    def __init__(self, api: Optional[BitgetAPI] = None):
        self.api = api
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

    def _initialize_cache(self, lookback_minutes: int = 50) -> None:
        """초기 캐시 구성 - API에서 최근 데이터 로드 및 DB 저장"""
        try:
            logger.info("Starting cache initialization...")
            
            # API로 과거 데이터 조회
            kline_data = self.api.get_kline_history(
                symbol='BTCUSDT',
                granularity='1m',
                limit=lookback_minutes
            )
            
            if kline_data and kline_data.get('code') == '00000':
                candles = kline_data.get('data', [])
                cursor = self.db.cursor()
                
                for candle in candles:
                    # API 응답 형식에 맞춰 파싱
                    timestamp = int(candle[0])  # Unix timestamp in milliseconds
                    open_price = float(candle[1])
                    high_price = float(candle[2])
                    low_price = float(candle[3])
                    close_price = float(candle[4])
                    volume = float(candle[5])
                    
                    # 캐시에 저장
                    candle_obj = Candle(
                        timestamp=timestamp,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        volume=volume
                    )
                    self.candles_cache[timestamp] = candle_obj
                    
                    # DB에 저장
                    cursor.execute("""
                        INSERT INTO kline_1m 
                        (timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=%s, high=%s, low=%s, close=%s, volume=%s
                    """, (
                        timestamp, open_price, high_price, low_price, close_price, volume,
                        open_price, high_price, low_price, close_price, volume
                    ))
                
                self.db.commit()
                cursor.close()
                
                logger.info(f"Successfully initialized cache with {len(candles)} historical candles")
                
                # 최신 캔들 설정
                if candles:
                    latest = candles[0]  # 가장 최근 캔들
                    self.latest_candle = Candle(
                        timestamp=int(latest[0]),
                        open=float(latest[1]),
                        high=float(latest[2]),
                        low=float(latest[3]),
                        close=float(latest[4]),
                        volume=float(latest[5])
                    )
                    
            else:
                logger.error("Failed to fetch historical candle data")
                
        except Exception as e:
            logger.error(f"Error initializing cache: {e}")

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

    def get_recent_candles(self, lookback: int = 50) -> List[Candle]:
        """최근 N개의 캔들 데이터 조회"""
        sorted_timestamps = sorted(self.candles_cache.keys(), reverse=True)
        return [self.candles_cache[ts] for ts in sorted_timestamps[:lookback]]

    def get_price_data_as_df(self, lookback: int = 50) -> pd.DataFrame:
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

    def calculate_stoch_rsi(self, period: int = 14, smoothk: int = 3, smoothd: int = 3) -> Tuple[float, float]:
        """
        Stochastic RSI 계산
        """
        try:
            # lookback 기간을 period의 3배 정도로 설정하여 충분한 데이터 확보
            df = self.get_price_data_as_df(lookback=period*3)
            
            if len(df) < period*2:  # 최소 필요 데이터 체크
                logger.warning(f"Stoch RSI 계산을 위한 데이터 부족: {len(df)} < {period*2}")
                return 50.0, 50.0  # 데이터 부족시 중립값 반환
            
            # RSI 계산
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # Stochastic RSI 계산
            rsi_min = rsi.rolling(window=period).min()
            rsi_max = rsi.rolling(window=period).max()
            stoch_rsi = 100 * (rsi - rsi_min) / (rsi_max - rsi_min)
            
            # K값 (Fast Stochastic)
            k = stoch_rsi.rolling(window=smoothk).mean()
            
            # D값 (Slow Stochastic)
            d = k.rolling(window=smoothd).mean()
            
            # nan 체크 및 처리
            k_value = k.iloc[-1]
            d_value = d.iloc[-1]
            
            if np.isnan(k_value) or np.isnan(d_value):
                logger.warning("Stoch RSI 계산 결과가 NaN입니다. 중립값으로 대체합니다.")
                return 50.0, 50.0
                
            return k_value, d_value
                
        except Exception as e:
            logger.error(f"Stoch RSI 계산 중 에러 발생: {e}")
            return 50.0, 50.0

    def calculate_technical_indicators(self, lookback: int = 50) -> Dict[str, float]:
        """기술적 지표 계산"""
        try:
            df = self.get_price_data_as_df(lookback)
            
            if len(df) < lookback:
                logger.warning(f"데이터 부족: {len(df)} < {lookback}")
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
            
            logger.info(f"계산된 지표: {result}")  # 이 로그 추가
            return result
            
        except Exception as e:
            logger.error(f"지표 계산 중 에러 발생: {e}")
            return {}
    
    def calculate_atr(self, period: int = 14) -> float:
        """
        Average True Range (ATR) 계산
        """
        try:
            logger.info(f"ATR 계산 시작 (기간: {period})")
            df = self.get_price_data_as_df(lookback=period*2)
            
            if len(df) < period:
                logger.warning(f"ATR 계산을 위한 데이터 부족: {len(df)} < {period}")
                return 0.0
                
            # True Range 계산
            df['high_low'] = df['high'] - df['low']
            df['high_pc'] = abs(df['high'] - df['close'].shift(1))
            df['low_pc'] = abs(df['low'] - df['close'].shift(1))
            
            df['tr'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)
            
            # ATR 계산
            atr = df['tr'].rolling(window=period).mean().iloc[-1]
            
            logger.info(f"계산된 ATR 값: {atr:.2f}")
            logger.debug(f"True Range 통계: 최소={df['tr'].min():.2f}, "
                        f"최대={df['tr'].max():.2f}, "
                        f"평균={df['tr'].mean():.2f}")
            
            return float(atr)
            
        except Exception as e:
            logger.error(f"ATR 계산 중 에러: {e}")
            return 0.0
                
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