import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional, Tuple
from database_manager import DatabaseManager, Candle
from data_api import BitgetAPI

logger = logging.getLogger(__name__)

class MarketDataManager:
    def __init__(self, api: BitgetAPI):
        self.api = api
        self.db_manager = DatabaseManager()
        self.latest_candle: Optional[Candle] = None
        self.candles_cache: Dict[int, Candle] = {}

    async def initialize(self):
        """비동기 초기화"""
        await self._initialize_cache()

    async def _initialize_cache(self, lookback_minutes: int = 200) -> None:
        """초기 캐시 구성"""
        try:
            logger.info("Starting cache initialization from DB...")
            candles = self.db_manager.get_recent_candles(lookback_minutes)
            
            for candle in candles:
                self.candles_cache[candle.timestamp] = candle
            
            if candles:
                self.latest_candle = candles[0]  # 가장 최근 캔들
                
            logger.info(f"Successfully initialized cache with {len(candles)} candles")
                
        except Exception as e:
            logger.error(f"Error initializing cache from DB: {e}")

    async def update_latest_candle(self, candle: Candle) -> None:
        """새로운 캔들 데이터로 캐시 업데이트"""
        self.latest_candle = candle
        self.candles_cache[candle.timestamp] = candle
        
        # DB에 저장
        self.db_manager.store_candle(candle)

        # 캐시 크기 관리
        if len(self.candles_cache) > 200:
            oldest_timestamp = min(self.candles_cache.keys())
            del self.candles_cache[oldest_timestamp]

    def get_latest_price(self) -> float:
        """현재 가격 조회"""
        return self.latest_candle.close if self.latest_candle else 0.0

    def get_recent_candles(self, lookback: int) -> List[Candle]:
        """최근 N개의 캔들 데이터 조회"""
        sorted_timestamps = sorted(self.candles_cache.keys(), reverse=True)
        return [self.candles_cache[ts] for ts in sorted_timestamps[:lookback]]

    def get_price_data_as_df(self, lookback: int) -> pd.DataFrame:
        """최근 N개의 캔들 데이터를 DataFrame으로 변환"""
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

    # 기술적 지표 계산 메서드들은 그대로 유지
    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        """EMA 계산"""
        return df['close'].ewm(span=period, adjust=False).mean()

    def calculate_stoch_rsi(self, period: int = 42, smoothk: int = 3, smoothd: int = 3) -> Tuple[float, float]:
        """Stochastic RSI 계산"""
        try:
            df = self.get_price_data_as_df(lookback=period*3)
            
            if len(df) < period*2:
                logger.warning(f"Insufficient data for Stoch RSI: {len(df)} < {period*2}")
                return 50.0, 50.0
            
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
            
            k = stoch_rsi.rolling(window=smoothk).mean()
            d = k.rolling(window=smoothd).mean()
            
            k_value = k.iloc[-1]
            d_value = d.iloc[-1]
            
            if np.isnan(k_value) or np.isnan(d_value):
                return 50.0, 50.0
                
            return k_value, d_value
                
        except Exception as e:
            logger.error(f"Error calculating Stoch RSI: {e}")
            return 50.0, 50.0

    def calculate_technical_indicators(self, lookback: int = 200) -> Dict[str, float]:
        """기술적 지표 계산"""
        try:
            df = self.get_price_data_as_df(lookback)
            
            if len(df) < lookback:
                logger.warning(f"Insufficient data: {len(df)} < {lookback}")
                return {}

            # 지표 계산
            ema7 = self.calculate_ema(df, 7).iloc[-1]
            ema25 = self.calculate_ema(df, 25).iloc[-1]
            ema200 = self.calculate_ema(df, 200).iloc[-1]
            price_change = df['close'].diff().iloc[-1]
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
            
            # 계산 결과 로깅 추가
            logger.info(f"Technical Indicators Calculated:\n"
                        f"  EMA200: {ema200:.2f}\n"
                        f"  Price Change: {price_change:.2f}\n"
                        f"  Stoch K: {stoch_k:.2f}\n"
                        f"  Stoch D: {stoch_d:.2f}\n"
                        f"  Last Close: {df['close'].iloc[-1]:.2f}\n"
                        f"  Last Volume: {df['volume'].iloc[-1]:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}

    def calculate_atr(self, period: int = 14) -> float:
        """ATR 계산"""
        try:
            df = self.get_price_data_as_df(lookback=period*2)
            
            if len(df) < period:
                return 0.0
                
            df['high_low'] = df['high'] - df['low']
            df['high_pc'] = abs(df['high'] - df['close'].shift(1))
            df['low_pc'] = abs(df['low'] - df['close'].shift(1))
            
            df['tr'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)
            atr = df['tr'].rolling(window=period).mean().iloc[-1]
            
            return float(atr)
            
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return 0.0