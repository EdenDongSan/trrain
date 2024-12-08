import asyncio
from data_web import BitgetWebsocket
from data_api import BitgetAPI
import os
from dotenv import load_dotenv
import logging
from order_execution import OrderExecutor
from trading_strategy_implementation import TradingStrategy
from market_data_manager import MarketDataManager

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    # Load environment variables
    load_dotenv()
    
    # API 설정
    api_key = os.getenv('BITGET_ACCESS_KEY')
    secret_key = os.getenv('BITGET_SECRET_KEY')
    passphrase = os.getenv('BITGET_PASSPHRASE')
    
    # 컴포넌트 초기화
    api = BitgetAPI(api_key, secret_key, passphrase)
    market_data = MarketDataManager()
    order_executor = OrderExecutor(api)
    strategy = TradingStrategy(market_data, order_executor)
    ws = BitgetWebsocket(market_data_manager=market_data)  # MarketDataManager 인스턴스 전달
    
    try:
        # 웹소켓 연결
        await ws.connect()
        
        # 동시 실행을 위한 태스크 생성
        ws_task = asyncio.create_task(ws.subscribe_kline())
        strategy_task = asyncio.create_task(strategy.run())
        
        # 모든 태스크가 완료될 때까지 대기
        await asyncio.gather(ws_task, strategy_task)
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await ws.disconnect()
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        await ws.disconnect()

if __name__ == "__main__":
    asyncio.run(main())