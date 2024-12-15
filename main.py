import asyncio
import signal
import sys
from data_web import BitgetWebsocket
from data_api import BitgetAPI
import os
from dotenv import load_dotenv
import logging
from order_execution import OrderExecutor
from trading_strategy_implementation import TradingStrategy
from market_data_manager import MarketDataManager
from logging_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # API 설정
        self.api_key = os.getenv('BITGET_ACCESS_KEY')
        self.secret_key = os.getenv('BITGET_SECRET_KEY')
        self.passphrase = os.getenv('BITGET_PASSPHRASE')
        
        # 컴포넌트 초기화
        self.api = BitgetAPI(self.api_key, self.secret_key, self.passphrase)
        self.market_data = MarketDataManager(api=self.api)
        self.ws = BitgetWebsocket(market_data_manager=self.market_data)
        self.order_executor = OrderExecutor(self.api)
        self.strategy = TradingStrategy(self.market_data, self.order_executor)
        
        
        self.is_running = False
        self.tasks = []
        self._cleanup_done = asyncio.Event()

    async def cleanup(self):
        """프로그램 종료 시 정리 작업 수행"""
        if not self.is_running:
            return
            
        logger.info("프로그램 종료 시작...")
        self.is_running = False
        
        try:
            # 실행 중인 태스크 취소
            for task in self.tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
            
            # 웹소켓 연결 종료
            try:
                if self.ws:
                    await self.ws.disconnect()
                logger.info("웹소켓 연결 종료 완료")
            except Exception as e:
                logger.error(f"웹소켓 종료 중 오류: {e}")
                
        except Exception as e:
            logger.error(f"정리 작업 중 오류 발생: {e}")
        finally:
            self._cleanup_done.set()
            logger.info("프로그램 종료 완료")

    async def start(self):
        """트레이딩 봇 시작"""
        try:
            self.is_running = True
            
            # 웹소켓 연결 및 초기 데이터 저장.
            await self.ws.connect()
            await self.ws.store_initial_candles()
            await self.market_data.initialize() #여기서 캐시초기화해버리면 200개 가져올수있죠~~~~~~~~~~db와 api 분리완료 고생했다..
            
            # 태스크 생성
            self.tasks = [
                asyncio.create_task(self.ws.subscribe_kline()),
                asyncio.create_task(self.strategy.run())
            ]
            
            # 태스크 완료 대기
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
        except asyncio.CancelledError:
            logger.info("프로그램 실행 취소됨")
        except Exception as e:
            logger.error(f"실행 중 오류 발생: {e}")
        finally:
            await self.cleanup()
            await self._cleanup_done.wait()

def main():
    bot = TradingBot()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    def signal_handler():
        logger.info("종료 시그널 수신...")
        for task in asyncio.all_tasks(loop):
            task.cancel()
    
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    except NotImplementedError:
        # Windows에서는 signal.signal 사용
        signal.signal(signal.SIGINT, lambda s, f: signal_handler())
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler())
    
    try:
        loop.run_until_complete(bot.start())
    except KeyboardInterrupt:
        logger.info("사용자에 의한 프로그램 종료")
        loop.run_until_complete(bot.cleanup())
    except Exception as e:
        logger.error(f"예기치 않은 오류 발생: {e}")
    finally:
        loop.close()

if __name__ == "__main__":
    main()