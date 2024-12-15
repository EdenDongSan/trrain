import asyncio
import websockets
import json
import logging
import mysql.connector
import os
from datetime import datetime
from websockets.protocol import State
from data_api import BitgetAPI
from market_data_manager import MarketDataManager, Candle


logger = logging.getLogger(__name__)

class BitgetWebsocket:
    def __init__(self, api: BitgetAPI, market_data: MarketDataManager):
        self.WS_URL = "wss://ws.bitget.com/v2/ws/public"
        self.ws = None
        self.market_data = market_data
        self.api = api
        self.db = self._setup_database()
        self.connected = False
        self.reconnecting = False
        self.subscriptions = []
        self._processing = False  # 메시지 처리 중인지 확인하는 플래그 추가

    def _setup_database(self):         # 안녕, 데이터 베이스 연결설정하는 함수야. 재연결때때  무한루프 함수에서  호출된다. 그리고 생성자 호출때 직접 호출되어서 db연결을 바로한다.
        """데이터베이스 연결 설정"""
        try:
            db = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD')
            )
            
            cursor = db.cursor()
            db_name = os.getenv('MYSQL_DATABASE')                  #DB 생성 및 체크아웃., column 부분 quote_volume은 usdt 기준 거래량임.
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")
            # 테이블과 column 만들기 부분.
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
            db.commit() # 수정완료를 요청.
            return db
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    async def store_initial_candles(self, symbol: str = 'BTCUSDT'):          #이거 메인에서 호출하면되네. 나 바보인가. 수정완. 아근데 이거 api쪽으로 가는게 맞는거같은데.... 근데 그러면 db연결부분에서 문제생기려나. 어차피 bitgetapi 생성자에 db부분 만들어야할거같으니까 도찐개찐인가. 이래서 db를 따로 모듈화 시켜놓고 임포트 시켜서 쓰는걸 구현해야하는건가.
        """초기 200개의 1분봉 데이터를 DB에 저장"""
        try:
            logger.info("Storing initial candle data to DB")
            
            # API를 통해 초기 캔들 데이터 조회
            response = await self.api.get_historical_candles(symbol)        #bitgetapi 임포트하고, 생성자 함수에서도 매개변수를 api=bitgetapi로 해놔야.. self.api=api 라고 인스턴스 생성가능하다. 그래야 이렇게 함수 호출이 가능하다...
            
            if response and response.get('code') == '00000':
                candles_data = response.get('data', [])
                
                # DB 연결 확인
                self.db.ping(reconnect=True)
                cursor = self.db.cursor()
                
                # 데이터 저장 row 형식으로 저장하면 그만, market_data가서 candle @dataclass를 사용해서 candle로 구현해서 데이터처리를 한다.
                for candle in candles_data:
                    cursor.execute("""
                        INSERT INTO kline_1m 
                        (timestamp, open, high, low, close, volume, quote_volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
                    """, (
                        int(candle[0]),     # timestamp
                        float(candle[1]),   # open
                        float(candle[2]),   # high
                        float(candle[3]),   # low
                        float(candle[4]),   # close
                        float(candle[5]),   # volume (base)
                        float(candle[6]),   # quote_volume (USDT)
                        float(candle[1]),   # open
                        float(candle[2]),   # high
                        float(candle[3]),   # low
                        float(candle[4]),   # close
                        float(candle[5]),   # volume (base)
                        float(candle[6])    # quote_volume (USDT)
                    ))
                
                self.db.commit()
                cursor.close()
                logger.info(f"Successfully stored {len(candles_data)} historical candles in DB")
            else:
                logger.error(f"Failed to fetch historical candles: {response}")
                
        except Exception as e:
            logger.error(f"Error storing initial candles: {e}")

    async def connect(self):     #세번째 동작함수. 연결 재요청..? 및 연결 요청이라고 할 수도 있을듯하다. 와 근데 이거 메인에서도 호출한다. 처음 시작할때; 음..상관없을듯. 재요청느낌으로 여기선.
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
                    
                asyncio.create_task(self._keep_alive())            #아 여기서 세번째-첫번째 동작함수가 나온다.
                return True
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                await asyncio.sleep(5)  # 5초 후 재시도
            finally:
                self.reconnecting = False
        return False

    async def disconnect(self):          # main에서 클린업함수 시작할때 쓰인다 삭제하면 안댐.
        """WebSocket 연결 종료"""
        if self.ws:
            try:
                await self.ws.close()
                self.connected = False
                logger.info("WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")

    async def is_connected(self):                                 # 첫번째 동작함수.
        """웹소켓 연결 상태 확인"""
        return (self.ws is not None and 
                hasattr(self.ws, 'state') and 
                self.ws.state == State.OPEN)

    async def _keep_alive(self):            #네가 연결유지를 시켜주는구나.세번째-첫번째 동작함수. else쪽에서 계속 오류뜨면 재연결 시도를 해준다.
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

    async def subscribe_kline(self, symbol: str = 'BTCUSDT'):              #메인에서 첫번째 실행태스크. 얘는 결국 _process_messages라는 무한루프 함수를 실행시키기 위해서 존재.
        """K라인 데이터 구독"""
        if symbol not in self.subscriptions:
            self.subscriptions.append(symbol)              #심볼을 추가하여 구독목록을 관리한다.

        subscribe_data = {                #딕셔너리를 만들어 거래소에 보낼 구독 요청 형식을 준비한다.
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": symbol
            }]
        }
        
        try:
            if await self.is_connected():                 #첫번째 동작함수 호출. 라이브러리를 많이 사용한다. - 얘는 연결을 계속해서 확인할 수 있게 해주는 역할을 한다.
                await self.ws.send(json.dumps(subscribe_data))           #연결되어있다면, 구독요청을JSON형식으로 변환하여 전송한다.
                logger.info(f"Subscription request sent: {subscribe_data}")
                # process_messages가 이미 실행 중이 아닐 때만 새로 시작
                if not self._processing:              #메시지 처리가 아직 시작되지 않는다면, 새로운 비동기 태스크로 _process_messages를 시작한다.
                    asyncio.create_task(self._process_messages())        #두번째 동작함수 호출. 태스크를 실제 실행시키는것이다.
            else:
                logger.warning("WebSocket not connected, attempting reconnection...")
                await self.connect()           #아 여기가 연결되어 있지않다면 세번째 동작함수를 호출하는 부분이다.
        except Exception as e:
            logger.error(f"Error subscribing to kline: {e}")
            await self.connect()

    async def _process_messages(self):                  #두번째 동작함수. 웹소켓으로부터 오는 모든 메시지를 지속적으로 처리하는 무한루프함수. 얘가 진짜 data_web의 기능을 한다.
        """메시지 처리"""
        if self._processing:
            return
            
        self._processing = True
        try:
            while await self.is_connected():              #첫번째 동작함수를 왜 또 호출하지? a: 이거 무한루프 함수라 계속 연결되어있는지 확인해야함. 그래야 넘어가게 해야 안전함.
                try:
                    message = await self.ws.recv()       # ws.recv로 웹소켓 메세지를 비동기적으로 수신한다.
                    if message == 'pong':                    # pong은 연결 유지용이므로 무시한다.
                        continue
                        
                    data = json.loads(message)                    # 받은 메세지를 JSON형식으로 파싱한다.
                    logger.debug(f"Received data: {data}")
                    
                    # DB 연결 확인 및 재연결
                    try:
                        self.db.ping(reconnect=True)            #매 메시지 처리마다 DB연결상태를 확인한다.
                    except Exception as e:
                        logger.error(f"Database connection error: {e}")
                        self.db = self._setup_database()             #재연결을 시도하는 함수 호출.
                    
                    if data.get('action') == 'update' and 'data' in data:
                        await self._handle_kline_data(data['data'])       # update액션이고 데이터가 있는 메세지만 처리한다. 함수로 캔들데이터 처리.

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed, attempting reconnect...")
                    self.connected = False
                    await asyncio.sleep(5)                      # 연결이 끊어지면 5초후 재연결시도, 다른 예외 발생시 1초 대기후 계속.
                    await self.connect()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await asyncio.sleep(1)
                    
        finally:
            self._processing = False

    async def _handle_kline_data(self, candle_data_list):        # 무한루프 함수에서 호출. update 액션이며, 비동기여야만하네요.
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
                    
                    # DB에 저장, 콘솔에도 보여줌.
                    cursor.execute("""
                        INSERT INTO kline_1m (timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open=VALUES(open), high=VALUES(high), low=VALUES(low),
                        close=VALUES(close), volume=VALUES(volume)
                    """, (timestamp, open_price, high_price, low_price, close_price, volume))
                    
                    self.db.commit()
                    # 캐시 업데이트를 위한 구현체 등장..
                    candle = Candle(
                    timestamp=timestamp,
                    open=open_price,
                    high=high_price, 
                    low=low_price,
                    close=close_price,
                    volume=volume
                )
                
                # market_data_manager의 캐시 업데이트           여기 순환참조부분인데,,, 비동기니까 어떻게든 돌아가지않을까....
                await self.market_data.update_latest_candle(candle)

            except Exception as e:
                logger.error(f"Error processing candle data: {e}, data: {candle_data}")
                continue