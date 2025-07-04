import json
import logging
import asyncio
import time
from typing import Dict, Any, Optional, Tuple
from compare import initialize_market_data, process_websocket_message, print_prices_periodically, initialize_globals
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI
from kalshi.wss import KalshiWSS, env, KEYID, private_key
from config import poly_asset_ids_to_subscribe, kalshi_tickers_to_subscribe, RUN_DURATION_MINUTES, JSON_OUTPUT_FILE_NAME

# Opening JSON file



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    
    message_queue = asyncio.Queue()

    await initialize_market_data()

    polymarket_wss = PolymarketWSS(
        POLYMARKET_MARKET_WSS_URI, 
        poly_asset_ids_to_subscribe,
        message_queue
    )

    kalshi_wss = KalshiWSS(
        key_id=KEYID,
        private_key=private_key,
        environment=env,
        message_queue=message_queue,
        ticker_list=kalshi_tickers_to_subscribe
    )
    
    # Connect to websockets
    await kalshi_wss.connect()
    await polymarket_wss.connect()

    if kalshi_wss.ws and polymarket_wss.websocket:
        # Start listening in the background
        kalshi_listener_task = asyncio.create_task(kalshi_wss.listen())
        polymarket_listener_task = asyncio.create_task(polymarket_wss.listen())
        
        # Task to consume messages from the queue
        async def message_consumer():
            while True:
                source, message = await message_queue.get()
                logger.debug(f"\n--- Main received message from {source} ---")
                # This will now trigger the JSON logging as well
                asyncio.create_task(process_websocket_message(source, message))
                message_queue.task_done()

        consumer_task = asyncio.create_task(message_consumer())
        
        # Start the periodic printing task
        printer_task = asyncio.create_task(print_prices_periodically())

        logger.info(f"WebSocket listeners started. Running for {RUN_DURATION_MINUTES} minutes...")
        start_time = time.time()
        try:
            await asyncio.sleep(RUN_DURATION_MINUTES * 60)
            logger.info(f"Run duration of {RUN_DURATION_MINUTES} minutes completed.")
        except asyncio.CancelledError:
            logger.info("Program cancelled (e.g., Ctrl+C detected or explicit stop).")
        finally:
            logger.info("Shutting down...")
            kalshi_listener_task.cancel()
            polymarket_listener_task.cancel()
            consumer_task.cancel()
            printer_task.cancel()

            await asyncio.gather(
                kalshi_listener_task, 
                polymarket_listener_task, 
                consumer_task, 
                printer_task,
                return_exceptions=True
            )
            
            await kalshi_wss.disconnect()
            await polymarket_wss.disconnect()
            
            # Not saving to file on exit anymore, as it's continuously logged
            # Instead, we just confirm closure of the JSON file stream (implicitly done by `with open`)
            logger.info(f"Data has been continuously logged to {JSON_OUTPUT_FILE_NAME}")

    else:
        logger.error("Could not start listener, connection to one or more WebSockets failed.")

if __name__ == "__main__":
    try:
        # Remove the old text output file if it exists, to start fresh JSONL
        try:
            import os
            if os.path.exists(JSON_OUTPUT_FILE_NAME):
                os.remove(JSON_OUTPUT_FILE_NAME)
                logger.info(f"Removed old {JSON_OUTPUT_FILE_NAME} to start fresh.")
        except Exception as e:
            logger.warning(f"Could not remove old JSON log file: {e}")

        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program manually interrupted (Ctrl+C). Check JSON log file for data.")
    except Exception as e:
        logger.exception("An unexpected error occurred. Check JSON log file for data.")