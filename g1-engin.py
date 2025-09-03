import requests
import json
import time
import logging
import asyncio
import pytz
import websockets
import random
import string
from datetime import datetime, timedelta
from decimal import Decimal
from aiohttp import web

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Global In-Memory Storage for Signals ---
signals_storage = []
# --- Helper Functions ---
def load_config():
    """Loads the configuration from config.json file."""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical("FATAL: config.json not found. Please create it.")
        exit()

# --- Signal Management (In-Memory) ---
def generate_complex_signal_id():
    """
    Generates a unique signal ID by multiplying 7 two-digit numbers and
    inserting 12 random letters into the resulting product string.
    """
    # 1. هفت عدد دو رقمی (بین ۱۰ تا ۹۹) در هم ضرب می‌شوند
    product = 1
    for _ in range(7):
        product *= random.randint(10, 99)

    # 2. دوازده حرف تصادفی (کوچک و بزرگ) ایجاد می‌شوند
    random_letters = [random.choice(string.ascii_letters) for _ in range(12)]

    # 3. حاصل ضرب به رشته تبدیل شده و برای درج حروف آماده می‌شود
    product_list = list(str(product))

    # 4. حروف تصادفی در مکان‌های تصادفی در بین ارقام درج می‌شوند
    for letter in random_letters:
        insert_position = random.randint(0, len(product_list))
        product_list.insert(insert_position, letter)
    
    # 5. لیست به رشته نهایی تبدیل شده و بازگردانده می‌شود
    return "".join(product_list)

def add_new_signal(symbol, action, entry_price, target_price, percentage_diff):
    """
    Generates a unique ID using the complex method, creates a signal object,
    and adds it to the in-memory list.
    """
    # ساخت شناسه جدید با روش پیچیده
    new_signal_id = generate_complex_signal_id()

    # ساخت دیکشنری سیگنال
    signal = {
        'signal_id': new_signal_id,
        'symbol': symbol,
        'action': action,
        'entry_price': entry_price,
        'target_price': target_price,
        'percentage_diff': round(percentage_diff, 2),
        'timestamp': datetime.now(pytz.utc).isoformat(),
        'status': 'new'
    }
    
    signals_storage.append(signal)
    # برای جلوگیری از پر شدن حافظه، می‌توانیم لیست را به ۱۰۰ سیگنال آخر محدود کنیم
    if len(signals_storage) > 100:
        signals_storage.pop(0)

    logger.info(f"Successfully added signal ID {new_signal_id} for {symbol}.")


def check_for_recent_signal(symbol, action, window_minutes):
    """Checks if a similar signal was sent within the defined time window from memory."""
    threshold_time = datetime.now(pytz.utc) - timedelta(minutes=window_minutes)
    for signal in reversed(signals_storage):
        if signal['symbol'] == symbol and signal['action'] == action:
            signal_time = datetime.fromisoformat(signal['timestamp'])
            if signal_time > threshold_time:
                return True
    return False

# --- WebSocket & API Functions ---
class WallexWebsocketManager:
    """Manages WebSocket connection to Wallex for real-time ask data."""
    def __init__(self, url):
        self.url = url
        self.order_books = {}
        self.connection = None
        self._is_running = False

    async def connect_and_listen(self):
        self._is_running = True
        while self._is_running:
            try:
                logger.info(f"Connecting to Wallex WebSocket at {self.url}...")
                async with websockets.connect(self.url) as ws:
                    self.connection = ws
                    logger.info("Successfully connected to Wallex WebSocket.")
                    if self.order_books:
                         await self.subscribe_to_streams(list(self.order_books.keys()))
                    await self._listen()
            except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
                logger.warning(f"Wallex WebSocket connection lost: {e}. Reconnecting in 15 seconds...")
                self.connection = None
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"An unexpected error occurred with Wallex WebSocket: {e}", exc_info=True)
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
                            if symbol not in self.order_books:
                                self.order_books[symbol] = {}
                            processed_orders = [[Decimal(order['price']), Decimal(order['quantity'])] for order in orders_data]
                            self.order_books[symbol]['asks'] = processed_orders
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
        except websockets.ConnectionClosed as e:
            logger.warning(f"Listen loop terminated as connection closed: {e}")
        except AttributeError:
             logger.warning("Listen loop aborted, connection object is not available.")

    async def subscribe_to_streams(self, symbols):
        if not self.connection:
             logger.warning("Cannot subscribe, WebSocket is not yet connected.")
             return
        logger.info(f"Subscribing to Sell-Depth for {len(symbols)} symbols...")
        for symbol in symbols:
            if symbol not in self.order_books:
                self.order_books[symbol] = {}
            subscription_message = ["subscribe", {"channel": f"{symbol}@sellDepth"}]
            try:
                await self.connection.send(json.dumps(subscription_message))
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send subscription for {symbol}@sellDepth: {e}")
                return
        logger.info("Finished sending all subscription requests.")

    def get_weighted_avg_ask_price(self, symbol, depth=5):
        order_book = self.order_books.get(symbol)
        if not order_book or not order_book.get('asks'):
            return None
        orders = order_book['asks'][:depth]
        if not orders: return None
        total_value = Decimal(0)
        total_volume = Decimal(0)
        for price, volume in orders:
            total_value += price * volume
            total_volume += volume
        return float(total_value / total_volume) if total_volume > 0 else None

    def stop(self):
        self._is_running = False

def get_coincatch_prices(config):
    """Fetches all last trade prices from CoinCatch."""
    try:
        url = config['price_sources']['coincatch']['base_url'] + config['price_sources']['coincatch']['tickers_endpoint']
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', [])
        
        prices = {
            ticker.get('symbol', '').replace('-', ''): float(price)
            for ticker in data
            if (price := ticker.get('close')) is not None
        }
        
        logger.info(f"Fetched {len(prices)} last trade prices from CoinCatch.")
        return {k: v for k, v in prices.items() if k and v}
    except Exception as e:
        logger.error(f"Error fetching CoinCatch prices: {e}")
        return {}

def get_wallex_usdt_markets(config):
    try:
        url = config['price_sources']['wallex']['base_url'] + config['price_sources']['wallex']['markets_endpoint']
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get("result", {}).get("symbols", {})
        return {s: d for s, d in data.items() if d.get('quoteAsset') == 'USDT'}
    except Exception as e:
        logger.error(f"Error fetching Wallex markets: {e}")
        return {}

# --- Main Analysis Logic ---
async def analysis_loop(config, ws_manager):
    """The main loop for analyzing prices and finding signals."""
    try:
        while True:
            logger.info("--- Starting New Cycle (Strategy: Wallex Ask vs CoinCatch Last Price) ---")
    
            coincatch_last_prices = get_coincatch_prices(config)
            if not coincatch_last_prices:
                logger.warning("Could not fetch CoinCatch prices. Skipping cycle.")
                await asyncio.sleep(config['settings']['check_interval_seconds'])
                continue

            wallex_symbols = list(ws_manager.order_books.keys())
            dedup_window = config['settings']['deduplication_window_minutes']
            signal_threshold = config['settings']['price_difference_threshold']
            
            logger.info("================== ANALYSIS ==================")
            for symbol in wallex_symbols:
                wallex_avg_ask = ws_manager.get_weighted_avg_ask_price(symbol)
                if not wallex_avg_ask:
                    continue

                coincatch_last_price = coincatch_last_prices.get(symbol)
                if not coincatch_last_price:
                    continue
                
                profit_pct = ((coincatch_last_price - wallex_avg_ask) / wallex_avg_ask) * 100
                log_message = f"| {symbol:<10} | Wallex Ask: {wallex_avg_ask:,.4f} $ | CoinCatch Last: {coincatch_last_price:,.4f} $ | Diff: {profit_pct:+.2f}%"
                logger.info(log_message)
                    
                if profit_pct >= signal_threshold:
                    logger.warning(f"Signal found for {symbol}: CoinCatch is {profit_pct:.2f}% more expensive")
                    action = "BUY"
                    if not check_for_recent_signal(symbol, action, dedup_window):
                        add_new_signal(symbol, action, wallex_avg_ask, coincatch_last_price, profit_pct)
                    else:
                        logger.info(f"Signal for {symbol} was a duplicate within the time window. Ignoring.")

            logger.info("================ END OF ANALYSIS ================\n")
            
            wait_time = config['settings']['check_interval_seconds']
            logger.info(f"--- Cycle Complete. Waiting for {wait_time} seconds. ---")
            await asyncio.sleep(wait_time)
    except asyncio.CancelledError:
        logger.info("Analysis loop cancelled.")

# --- Web Server Handlers ---
async def get_signals(request):
    """This function is the handler for the /signals API endpoint."""
    return web.json_response(signals_storage)

async def main():
    config = load_config()
    
    # WebSocket Manager Setup
    ws_manager = WallexWebsocketManager(config['price_sources']['wallex']['websocket_url'])
    ws_task = asyncio.create_task(ws_manager.connect_and_listen())
    
    wallex_usdt_markets = get_wallex_usdt_markets(config)
    if not wallex_usdt_markets:
        logger.critical("Could not fetch Wallex markets to subscribe. Exiting.")
        ws_manager.stop()
        await ws_task
        return
        
    await asyncio.sleep(5)
    await ws_manager.subscribe_to_streams(list(wallex_usdt_markets.keys()))
    
    # Analysis Loop Task
    analysis_task = asyncio.create_task(analysis_loop(config, ws_manager))

    # Web Server Setup
    app = web.Application()
    app.router.add_get('/signals', get_signals)
    runner = web.AppRunner(app)
    await runner.setup()
    # ------------------   تغییر پورت در خط زیر   ------------------
    site = web.TCPSite(runner, '0.0.0.0', 8888)
    # -----------------------------------------------------------
    await site.start()
    logger.info("Web server started on http://0.0.0.0:8888/signals")

    try:
        # Keep the main function alive to serve requests
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down...")
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