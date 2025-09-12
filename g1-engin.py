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
    coincatch_ok = False
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

    # Test CoinCatch HTTP API
    try:
        url = config['price_sources']['coincatch']['base_url'] + config['price_sources']['coincatch']['ticker_endpoint']
        params = {'symbol': 'BTCUSDT_SPBL'} # Use a sample symbol for testing
        logger.info(f"--> Testing connection to CoinCatch HTTP API at: {url}")
        response = await asyncio.to_thread(requests.get, url, params=params, timeout=timeout)
        response.raise_for_status()
        if response.json().get("code") == "00000":
            logger.info("✅ SUCCESS: CoinCatch HTTP API connection is OK.")
            coincatch_ok = True
        else:
            logger.error(f"❌ FAILED: CoinCatch API connected, but returned an error: {response.json().get('msg')}")
    except Exception as e:
        logger.error(f"❌ FAILED: Could not connect to CoinCatch HTTP API. Error: {e}")
    
    logger.info("--- Network Diagnostics Complete ---")
    return wallex_ok and coincatch_ok

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

def add_signal_to_history(symbol, action, entry_price, target_price, percentage_diff):
    """Adds a signal to the historical list for deduplication."""
    new_signal_id = generate_complex_signal_id()
    signal = {
        'signal_id': new_signal_id,
        'symbol': symbol,
        'action': action,
        'entry_price': entry_price,
        'target_price': target_price,
        'percentage_diff': round(percentage_diff, 2),
        'timestamp': datetime.now(pytz.utc).isoformat()
    }
    historical_signals.append(signal)
    if len(historical_signals) > 100:
        historical_signals.pop(0)
    logger.info(f"Signal for {symbol} added to history (ID: {new_signal_id}).")
    return signal

def check_for_recent_signal(symbol, action, window_minutes):
    """Checks if a similar signal was sent within the defined time window."""
    threshold_time = datetime.now(pytz.utc) - timedelta(minutes=window_minutes)
    for signal in reversed(historical_signals):
        if signal['symbol'] == symbol and signal['action'] == action:
            signal_time = datetime.fromisoformat(signal['timestamp'])
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
                    # On a fresh connection (or reconnection), resubscribe to all necessary streams.
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
            # Add symbol to order_books here so it's remembered for reconnection
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

    def get_simple_avg_top_3_ask_price(self, symbol):
        """Calculates the simple average price of the top 3 sell orders."""
        orders = self.order_books.get(symbol, [])
        if not orders or len(orders) < 3:
            return None
        top_3_orders = orders[:3]
        total_price = sum(Decimal(str(order['price'])) for order in top_3_orders)
        return float(total_price / 3)

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

def get_coincatch_prices(config, markets):
    """Fetches latest prices from CoinCatch for a list of markets, one by one."""
    if not markets: return {}
    timeout = 30
    all_prices = {}
    base_url = config['price_sources']['coincatch']['base_url']
    endpoint = config['price_sources']['coincatch']['ticker_endpoint']
    url = base_url + endpoint
    
    logger.info(f"Requesting prices for {len(markets)} markets from CoinCatch (one by one)...")
    
    for i, symbol in enumerate(markets):
        try:
            coincatch_symbol = f"{symbol}_SPBL"
            params = {'symbol': coincatch_symbol}
            
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            response_data = r.json()

            if response_data.get("code") == "00000" and response_data.get("data"):
                all_prices[symbol] = float(response_data['data']['close'])
            else:
                if i % 20 == 0:
                     logger.debug(f"Could not fetch price for {coincatch_symbol}: {response_data.get('msg')}")

            time.sleep(0.05) 

        except Exception as e:
            if i % 20 == 0:
                logger.debug(f"An error occurred while fetching CoinCatch price for {coincatch_symbol}: {e}")
            
    logger.info(f"Successfully fetched {len(all_prices)} prices from CoinCatch.")
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
            
            global_prices = get_coincatch_prices(config, wallex_markets)
            if not global_prices:
                logger.warning("Could not fetch any global prices from CoinCatch. Skipping this cycle.")
                await asyncio.sleep(config['settings']['check_interval_seconds'])
                continue

            dedup_window = config['settings']['deduplication_minutes']
            signal_threshold = config['settings']['price_difference_threshold']
            
            logger.info(f"--- Analyzing {len(wallex_markets)} markets ---")
            for symbol in wallex_markets:
                wallex_avg_ask = ws_manager.get_simple_avg_top_3_ask_price(symbol)
                if not wallex_avg_ask:
                    continue

                global_price = global_prices.get(symbol)
                if not global_price:
                    continue
                
                discount_pct = ((global_price - wallex_avg_ask) / global_price) * 100
                
                if abs(discount_pct) >= signal_threshold:
                    logger.info(f"-> {symbol:<10} | Wallex Avg Ask: {wallex_avg_ask:,.4f} | CoinCatch Price: {global_price:,.4f} | Difference: {discount_pct:+.2f}%")
                
                if discount_pct >= signal_threshold:
                    logger.warning(f"SIGNAL FOUND! {symbol} on Wallex is {discount_pct:.2f}% cheaper than CoinCatch.")
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
async def get_signals(request):
    """Handler for the /signals API endpoint."""
    return web.json_response(latest_cycle_signals)

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
    
    # --- CORRECTED LOGIC ORDER ---
    # 1. Start the WebSocket connection manager in the background.
    ws_task = asyncio.create_task(ws_manager.connect_and_listen())
    
    # 2. Give it a moment to establish the initial connection.
    logger.info("Waiting for WebSocket to establish initial connection...")
    await asyncio.sleep(5) 
    
    # 3. Now that the connection is likely established, send the subscription list.
    await ws_manager.subscribe_to_streams(wallex_markets)
    
    # 4. Start the analysis loop.
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