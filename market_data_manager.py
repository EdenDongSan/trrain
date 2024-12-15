import mysql.connector
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os
from data_web import BitgetWebsocket

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
# 생성자에서 호출된 모든 메서드는 객체 생성 시점에 실행된다. 그렇게도 실행시킬 수 있다는 말.
class MarketDataManager:
    def __init__(self, websocket: BitgetWebsocket):
        self.websocket = websocket
        self.db = self.websocket.db  # BitgetWebsocket의 DB 연결을 사용 api는 미사용하게 수정함.
        self.latest_candle: Optional[Candle] = None
        self.candles_cache: Dict[int, Candle] = {}
        

    def _connect_db(self) -> mysql.connector.MySQLConnection:              # 아 이거... 그냥 web에 있는 거 끌어다가 써야할거같은데, 객체로 받는한이 있더라도.
        """데이터베이스 연결"""
        return mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )

    async def initialize(self):              #응 비동기 초기화하고 start에 갖다놓으면 그만이야~~ 컴포넌트 초기화 할때 cache 초기화 안시키면 그만이야~~~~~~~~~~~~
        """비동기 초기화"""
        await self._initialize_cache()

    async def _initialize_cache(self, lookback_minutes: int = 200) -> None:
        """초기 캐시 구성 - DB에서 최근 데이터 로드"""
        try:
            logger.info("Starting cache initialization from DB...")
            cursor = self.db.cursor()
            
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume 
                FROM kline_1m 
                ORDER BY timestamp DESC 
                LIMIT %s
            """, (lookback_minutes,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            for row in rows:
                candle = Candle(
                    timestamp=row[0],
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5])
                )
                self.candles_cache[candle.timestamp] = candle
            
            if rows:
                self.latest_candle = self.candles_cache[rows[0][0]]
                
            logger.info(f"Successfully initialized cache with {len(rows)} candles from DB")
                
        except Exception as e:
            logger.error(f"Error initializing cache from DB: {e}")

    async def update_latest_candle(self, candle: Candle) -> None: # 이건 _handle_kline_data 의 update 성질을 활용해서 그냥 db랑 캐시랑 한방에 수정하는게 효율적. 하지만, 순환참조 발생가능성 있음.
        """새로운 캔들 데이터로 캐시 업데이트"""
        self.latest_candle = candle 
        self.candles_cache[candle.timestamp] = candle

        if len(self.candles_cache) > 200:
            oldest_timestamp = min(self.candles_cache.keys())
            del self.candles_cache[oldest_timestamp]

    def get_latest_price(self) -> float:              # 이거 리밋 청산 시도 때 호출된다. 지우면 안됨.
        """현재 가격 조회"""
        return self.latest_candle.close if self.latest_candle else 0.0

    def get_recent_candles(self, lookback) -> List[Candle]:        # 두번째 동작.
        """최근 N개의 캔들 데이터 조회"""
        sorted_timestamps = sorted(self.candles_cache.keys(), reverse=True)
        return [self.candles_cache[ts] for ts in sorted_timestamps[:lookback]]

    def get_price_data_as_df(self, lookback) -> pd.DataFrame:            # 첫번째 동작함수.
        """최근 N개의 캔들 데이터를 pandas DataFrame으로 반환"""
        candles = self.get_recent_candles(lookback)
        data = {
            'timestamp': [c.timestamp for c in candles],
            'open': [c.open for c in candles],
            'high': [c.high for c in candles],
            'low': [c.low for c in candles],
            'close': [c.close for c in candles],
            'volume': [c.volume for c in candles]
        }                                                                       #df란 캔들데이타를 말합니다.
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.sort_values('timestamp')

    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:              # 세번째 동작함수.
        """EMA 계산"""
        return df['close'].ewm(span=period, adjust=False).mean()

    def calculate_stoch_rsi(self, period: int = 14, smoothk: int = 3, smoothd: int = 3) -> Tuple[float, float]:        # 네번째 동작함수.
        """
        Stochastic RSI 계산
        """
        try:
            # lookback 기간을 period의 3배 정도로 설정하여 충분한 데이터 확보
            df = self.get_price_data_as_df(lookback=period*3)            #여기서는 lookback에 200개를 넣어주는게 아니라 필요한만큼만 가져온다.
            
            if len(df) < period*2:  # 최소 필요 데이터 체크
                logger.warning(f"Stoch RSI 계산을 위한 데이터 부족: {len(df)} < {period*2}")
                return 50.0, 50.0  # 데이터 부족시 중립값 반환
            
            # RSI 계산                                                            #여기서 rsi 계산을 하므로 rsi 계산함수는 필요 없음. rsi를 쓸게 아닌이상.
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

    def calculate_technical_indicators(self, lookback: int = 200) -> Dict[str, float]:              # 얘가, trading_strategy_impl에서 불러오는 실행함수.
        """기술적 지표 계산"""
        try:
            df = self.get_price_data_as_df(lookback)                    # 1,2번째 동작함수 연결. 여기서는 lookback에 200개 넣어주는게 맞다.
            # 이거 db에서 받아오게 수정해야만한다. 최근 n개의 캔들데이터를 인디케이터로 줄수있게하는거다.
            if len(df) < lookback:
                logger.warning(f"데이터 부족: {len(df)} < {lookback}")
                return {}

            # EMA 계산 (7, 25, 99)
            ema7 = self.calculate_ema(df, 7).iloc[-1]                            # 3번째 동작함수 연결.
            ema25 = self.calculate_ema(df, 25).iloc[-1]
            ema200 = self.calculate_ema(df, 200).iloc[-1]
        
            # 가격 변화 계산 (이전 봉 대비)
            price_change = df['close'].diff().iloc[-1]

            # Stochastic RSI 계산
            stoch_k, stoch_d = self.calculate_stoch_rsi()                        #4번째 동작함수를 연결함. 근데 df를 불러올때 첫번째 동작함수를 부르는듯? 수정필요할듯.

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
    
    def calculate_atr(self, period: int = 14) -> float:          # 이거 익절할때 쓰는 함수다.
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
                