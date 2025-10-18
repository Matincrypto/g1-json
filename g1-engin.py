import requests
import json
import logging
import asyncio
import pytz
import websockets
import random
import string
import time
from datetime import datetime, timedelta
from decimal import Decimal
from aiohttp import web
import sys
import aiohttp

#نسخه: 1.4 (به‌روز شده برای فرمت خروجی API)
#تاریخ: 16 سپتامبر 2025


# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# --- Global In-Memory Storage ---
historical_signals = []
latest_cycle_signals = []

# --- Helper Functions ---
def load_config():
    """Loads the configuration from config.json file."""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical("FATAL: config.json not found. Please create it.")
        exit()

# --- Network Diagnostics ---
async def run_network_diagnostics(config):
    """Tests connectivity to required API endpoints before starting."""
    logger.info("--- Running Network Diagnostics ---")
    wallex_ok = False
    binance_ok = False
    timeout = 30

    # Test Wallex HTTP API
    try:
        url = config['price_sources']['wallex']['base_url'] + config['price_sources']['wallex']['markets_endpoint']
        logger.info(f"--> Testing connection to Wallex HTTP API at: {url}")
        response = await asyncio.to_thread(requests.get, url, timeout=timeout)
        response.raise_for_status()
        if "result" in response.json():
            logger.info("✅ SUCCESS: Wallex HTTP API connection is OK.")
            wallex_ok = True
    except Exception as e:
        logger.error(f"❌ FAILED: Could not connect to Wallex HTTP API. Error: {e}")

    # Test Binance HTTP API
    try:
        url = config['price_sources']['binance']['base_url'] + config['price_sources']['binance']['ticker_endpoint']
        params = {'symbol': 'BTCUSDT'} # Use a sample symbol for testing
        logger.info(f"--> Testing connection to Binance HTTP API at: {url}")
        response = await asyncio.to_thread(requests.get, url, params=params, timeout=timeout)
        response.raise_for_status()
        if "price" in response.json():
            logger.info("✅ SUCCESS: Binance HTTP API connection is OK.")
            binance_ok = True
        else:
            logger.error(f"❌ FAILED: Binance API connected, but returned an unexpected response: {response.text}")
    except Exception as e:
        logger.error(f"❌ FAILED: Could not connect to Binance HTTP API. Error: {e}")
    
    logger.info("--- Network Diagnostics Complete ---")
    return wallex_ok and binance_ok

# --- Signal Management ---
def generate_complex_signal_id():
    """Generates a unique signal ID."""
    product = 1
    for _ in range(7):
        product *= random.randint(10, 99)
    random_letters = [random.choice(string.ascii_letters) for _ in range(12)]
    product_list = list(str(product))
    for letter in random_letters:
        insert_position = random.randint(0, len(product_list))
        product_list.insert(insert_position, letter)
    return "".join(product_list)

# --- START OF CHANGE 1 (Modified for full symbol name) ---
def add_signal_to_history(symbol, action, entry_price, target_price, percentage_diff):
    """Adds a signal to the historical list with the new API format."""
    
    # [!!] این خط تغییر کرده است تا نام کامل جفت ارز (symbol) را ذخیره کند
    asset_name = symbol 

    signal = {
      "asset_name": asset_name,
      "entry_price": entry_price,
      "exchange_name": "Wallex", # Hardcoded as per request
      "exit_price": target_price,
      "expected_profit_percentage": round(percentage_diff, 2),
      "strategy_name": "G1", # Hardcoded as per request
      # --- Internal fields for deduplication ---
      "_internal_timestamp": datetime.now(pytz.utc).isoformat(),
      "_internal_symbol": symbol,
      "_internal_action": action
    }
    historical_signals.append(signal)
    if len(historical_signals) > 100:
        historical_signals.pop(0)
    logger.info(f"Signal for {symbol} added to history.")
    return signal
# --- END OF CHANGE 1 ---

def check_for_recent_signal(symbol, action, window_minutes):
    """Checks if a similar signal was sent within the defined time window."""
    threshold_time = datetime.now(pytz.utc) - timedelta(minutes=window_minutes)
    for signal in reversed(historical_signals):
        # Use internal fields for checking duplicates
        if signal['_internal_symbol'] == symbol and signal['_internal_action'] == action:
            signal_time = datetime.fromisoformat(signal['_internal_timestamp'])
            if signal_time > threshold_time:
                return True
    return False

# --- WebSocket & API Functions ---
class WallexWebsocketManager:
    """Manages WebSocket connection to Wallex for real-time sellDepth data."""
    def __init__(self, url):
        self.url = url
        self.order_books = {}
        self.connection = None
        self._is_running = False

    async def connect_and_listen(self):
        self._is_running = True
        while self._is_running:
            try:
                logger.info(f"WebSocket: Attempting to connect to {self.url}...")
                async with websockets.connect(self.url) as ws:
                    self.connection = ws
                    logger.info("WebSocket: Connection successful.")
                    await self.subscribe_to_streams(list(self.order_books.keys()))
                    await self._listen()
            except (websockets.ConnectionClosed, ConnectionRefusedError, asyncio.TimeoutError) as e:
                logger.warning(f"WebSocket: Connection lost ({type(e).__name__}). Reconnecting in 15 seconds...")
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"WebSocket: An unexpected error occurred: {e}", exc_info=True)
                self._is_running = False

    async def _listen(self):
        try:
            async for message in self.connection:
                try:
                    data = json.loads(message)
                    if isinstance(data, list) and len(data) == 2:
                        channel_name, orders_data = data
                        if "@sellDepth" in channel_name:
                            symbol = channel_name.split('@')[0]
                            self.order_books[symbol] = orders_data
                except Exception as e:
                    logger.error(f"WebSocket: Error processing message: {e}")
        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket: Listen loop terminated as connection closed: {e}")

    async def subscribe_to_streams(self, symbols):
        if not self.connection:
            logger.warning("WebSocket: Cannot subscribe, connection object does not exist yet.")
            return

        logger.info(f"WebSocket: Subscribing to Sell-Depth for {len(symbols)} symbols...")
        for symbol in symbols:
            if symbol not in self.order_books:
                self.order_books[symbol] = []
            
            subscription_message = ["subscribe", {"channel": f"{symbol}@sellDepth"}]
            try:
                await self.connection.send(json.dumps(subscription_message))
                await asyncio.sleep(0.05)
            except websockets.exceptions.ConnectionClosed:
                logger.error(f"WebSocket: Failed to subscribe for {symbol} because connection is closed. Will retry on reconnect.")
                return
            except Exception as e:
                logger.error(f"WebSocket: Failed to send subscription for {symbol}: {e}")
        logger.info("WebSocket: Finished sending all subscription requests.")

    def get_simple_avg_top_5_ask_price(self, symbol):
        """Calculates the simple average price of the top 5 sell orders."""
        orders = self.order_books.get(symbol, [])
        if not orders or len(orders) < 5:
            return None
        top_5_orders = orders[:5]
        total_price = sum(Decimal(str(order['price'])) for order in top_5_orders)
        return float(total_price / 5)

    def stop(self):
        self._is_running = False

def get_wallex_usdt_markets(config):
    """Fetches all active USDT market symbols from Wallex."""
    timeout = 30
    try:
        url = config['price_sources']['wallex']['base_url'] + config['price_sources']['wallex']['markets_endpoint']
        logger.info(f"Requesting Wallex markets from {url} with a {timeout}s timeout...")
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json().get("result", {}).get("symbols", {})
        markets = [s for s, d in data.items() if d.get('quoteAsset') == 'USDT' and d.get('faName')]
        logger.info(f"Found {len(markets)} active USDT markets on Wallex.")
        return markets
    except Exception as e:
        logger.error(f"An error occurred while fetching Wallex markets: {e}")
        return []

async def fetch_binance_price_for_symbol(session, url, symbol):
    """یک تابع کمکی برای دریافت قیمت یک ارز از Binance به صورت غیرهمزمان."""
    params = {'symbol': symbol}
    try:
        async with session.get(url, params=params, timeout=10) as response:
            response.raise_for_status()
            response_data = await response.json()
            if 'price' in response_data:
                return symbol, float(response_data['price'])
            else:
                return symbol, None
    except Exception:
        return symbol, None

async def get_binance_prices(config, markets):
    """
    (نسخه جدید برای Binance)
    با استفاده از درخواست‌های همزمان، آخرین قیمت‌ها را از Binance دریافت می‌کند.
    """
    if not markets:
        return {}

    all_prices = {}
    base_url = config['price_sources']['binance']['base_url']
    endpoint = config['price_sources']['binance']['ticker_endpoint']
    url = base_url + endpoint

    logger.info(f"Requesting prices for {len(markets)} markets from Binance (concurrently)...")
    
    tasks = []
    async with aiohttp.ClientSession() as session:
        for symbol in markets:
            tasks.append(fetch_binance_price_for_symbol(session, url, symbol))
        
        results = await asyncio.gather(*tasks)

    processed_count = 0
    for symbol, price in results:
        if price is not None:
            all_prices[symbol] = price
            processed_count += 1
        else:
            if (len(results) - processed_count) % 20 == 0:
                 logger.debug(f"Could not fetch price for {symbol} from Binance.")

    logger.info(f"Successfully fetched {len(all_prices)} prices from Binance.")
    return all_prices


# --- Main Analysis Logic ---
async def analysis_loop(config, ws_manager, wallex_markets):
    """The main loop for analyzing prices and finding signals."""
    global latest_cycle_signals
    try:
        while True:
            logger.info("==================================================================")
            logger.info(f"               Starting New Analysis Cycle                       ")
            logger.info("==================================================================")
            current_cycle_signals = []
            
            global_prices = await get_binance_prices(config, wallex_markets)
            if not global_prices:
                logger.warning("Could not fetch any global prices from Binance. Skipping this cycle.")
                await asyncio.sleep(config['settings']['check_interval_seconds'])
                continue

            dedup_window = config['settings']['deduplication_window_minutes']
            signal_threshold = config['settings']['price_difference_threshold']
            
            logger.info(f"--- Analyzing {len(wallex_markets)} markets ---")
            for symbol in wallex_markets:
                wallex_avg_ask = ws_manager.get_simple_avg_top_5_ask_price(symbol)
                if not wallex_avg_ask:
                    continue

                global_price = global_prices.get(symbol)
                if not global_price:
                    continue
                
                discount_pct = ((global_price - wallex_avg_ask) / global_price) * 100
                
                if abs(discount_pct) >= signal_threshold:
                    logger.info(f"-> {symbol:<10} | Wallex Avg Ask: {wallex_avg_ask:,.4f} | Binance Price: {global_price:,.4f} | Difference: {discount_pct:+.2f}%")
                
                if discount_pct >= signal_threshold:
                    logger.warning(f"SIGNAL FOUND! {symbol} on Wallex is {discount_pct:.2f}% cheaper than Binance.")
                    action = "BUY_ON_WALLEX"
                    if not check_for_recent_signal(symbol, action, dedup_window):
                        new_signal = add_signal_to_history(symbol, action, wallex_avg_ask, global_price, discount_pct)
                        current_cycle_signals.append(new_signal)
                    else:
                        logger.info(f"DUPLICATE signal for {symbol}. Ignoring.")
            
            latest_cycle_signals = current_cycle_signals
            logger.info(f"--- Analysis Cycle Complete. Found {len(latest_cycle_signals)} new signals to display. ---")
            
            wait_time = config['settings']['check_interval_seconds']
            logger.info(f"Waiting for {wait_time} seconds until next cycle...")
            await asyncio.sleep(wait_time)
    except asyncio.CancelledError:
        logger.info("Analysis loop has been cancelled.")

# --- Web Server Handlers ---
# --- START OF CHANGE 2 ---
async def get_signals(request):
    """Handler for the /signals API endpoint with the new format."""
    
    # Create a clean list for the output, without internal fields
    opportunities_to_display = []
    for signal in latest_cycle_signals:
        display_signal = signal.copy()
        # Remove internal fields before showing to the user
        display_signal.pop("_internal_timestamp", None)
        display_signal.pop("_internal_symbol", None)
        display_signal.pop("_internal_action", None)
        opportunities_to_display.append(display_signal)

    response_data = {
        "last_updated": datetime.now(pytz.utc).isoformat(),
        "opportunities": opportunities_to_display
    }
    return web.json_response(response_data)
# --- END OF CHANGE 2 ---

async def main():
    config = load_config()
    
    if not await run_network_diagnostics(config):
        logger.critical("Network diagnostics failed. The application cannot start.")
        return

    wallex_markets = get_wallex_usdt_markets(config)
    if not wallex_markets:
        logger.critical("Could not fetch Wallex markets. Check API config. Exiting.")
        return

    ws_manager = WallexWebsocketManager(config['price_sources']['wallex']['websocket_url'])
    
    ws_task = asyncio.create_task(ws_manager.connect_and_listen())
    
    logger.info("Waiting for WebSocket to establish initial connection...")
    await asyncio.sleep(5) 
    
    await ws_manager.subscribe_to_streams(wallex_markets)
    
    analysis_task = asyncio.create_task(analysis_loop(config, ws_manager, wallex_markets))

    app = web.Application()
    app.router.add_get('/g1/signals', get_signals) 
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8888)
    await site.start()
    logger.info("Web server started on http://0.0.0.0:8888/g1/signals")

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down application...")
        analysis_task.cancel()
        ws_manager.stop()
        await ws_task
        await runner.cleanup()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user. Exiting.")
