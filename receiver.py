import asyncio
import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed
import json
import traceback
import threading

from entities import DepthUpdate, BID, ASK, KLine, Trade
from utils import IntSequence,AsyncIterQueue

ws_id_sequence = IntSequence()

def process_depth_update(msg,msg_receiver):
    last_update_id = msg['u']
    time = msg['E']
    symbol = msg['s']

    for price,qty in msg['b']:
        event = DepthUpdate(
            time = time,
            symbol = symbol,
            side = BID,
            price = price,
            qty = qty,
            last_update_id = last_update_id
        )

        msg_receiver(event)

    for price,qty in msg['a']:
        event = DepthUpdate(
            time = time,
            symbol = symbol,
            side = ASK,
            price = price,
            qty = qty,
            last_update_id = last_update_id
        )

        msg_receiver(event)

def process_kline_event(msg,msg_receiver):
    payload = msg['k']
    if not payload['x']:
        return

    kline = KLine(
        time = msg['E'],
        symbol = msg['s'],
        start_time = payload['t'],
        close_time = payload['T'],
        interval = payload['i'],
        open_price = payload['o'],
        close_price = payload['c'],
        high_price = payload['h'],
        low_price = payload['l'],
        base_asset_volume = payload['v'],
        num_of_trades = payload['n'],
        quote_asset_volume = payload['q'],
        taker_buy_base_asset_volume = payload['V'],
        taker_buy_quote_asset_volume = payload['Q']
    )

    msg_receiver(kline)

def process_trade_event(msg,msg_receiver):
    trade = Trade(
        time = msg['E'],
        symbol = msg['s'],
        trade_id = msg['t'],
        price = msg['p'],
        quantity = msg['q'],
        buyer_order_id = msg['b'],
        seller_order_id = msg['a'],
        trade_time = msg['T'],
        is_buyer_market_maker = msg['m']
    )

    msg_receiver(trade)

class MessageRouter:
    def __init__(self):
        self._routes = list()
    
    def add_route(self,route_predicate,processor,outgoing):
        self._routes.append((route_predicate,processor,outgoing))
    
    def put_message(self,message):
        for route_predicate,processor,outgoing in self._routes:
            if route_predicate(message):
                processor(message,outgoing)

class BookOrderPrefetcher:
    def __init__(self,outgoing,symbol):
        self._symbol = symbol
        self._outgoing = outgoing
        self._prefetch_mode = True
        self._prefetched_messages = list()
    
    def put_from_ws(self,message):
        if message.symbol != self._symbol:
            return
        
        if self._prefetch_mode:
            # cut long messages lists
            if len(self._prefetched_messages) > 100000:
                self._prefetched_messages = self._prefetched_messages[50000:]
            
            self._prefetched_messages.append(message)
        else:
            self._outgoing(message)
    
    def put_from_http(self,api_response):
        time = None
        last_update_id = api_response['lastUpdateId']
        for msg in self._prefetched_messages:
            if msg.last_update_id <= last_update_id:
                if time is None or msg.time > time:
                    time = msg.time
        
        self._prefetch_mode = False
        if time is None:
            for msg in self._prefetched_messages:
                self._outgoing(msg)
            self._prefetched_messages.clear()

        else:
            messages_after = sorted(filter(lambda msg:msg.last_update_id > last_update_id,self._prefetched_messages),key = lambda msg:msg.last_update_id)
            self._prefetched_messages.clear()

            for price,qty in api_response['bids']:
                event = DepthUpdate(
                    time = time,
                    symbol = self._symbol,
                    side = BID,
                    price = price,
                    qty = qty,
                    last_update_id = last_update_id
                )

                self._outgoing(event)

            for price,qty in api_response['asks']:
                event = DepthUpdate(
                    time = time,
                    symbol = self._symbol,
                    side = ASK,
                    price = price,
                    qty = qty,
                    last_update_id = last_update_id
                )

                self._outgoing(event)
            
            for event in messages_after:
                self._outgoing(event)
    
    def fail(self):
        self._prefetch_mode = True
        self._prefetched_messages.clear()
    
    def cancel_prefetch(self):
        self._prefetch_mode = False
        for msg in self._prefetched_messages:
            self._outgoing(msg)
        
        self._prefetched_messages.clear()

    @property
    def ws_receiver(self):
        return lambda msg:self.put_from_ws(msg)
    
    @property
    def http_receiver(self):
        return lambda msg:self.put_from_http(msg)

async def load_book_order(symbol,callback,sleep_time = None,cancel_callback = None):
    if not sleep_time is None:
        await asyncio.sleep(sleep_time)
    success = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/depth?symbol=" + symbol + "&limit=1000") as resp:
                if resp.status == 200:
                    resp_text = await resp.text()
                    callback(json.loads(resp_text))
                    success = True
                else:
                    print("Error during book order loading. HTTP response code -",resp.status)
                    print(resp.reason)
    finally:
        if not success and not cancel_callback is None:
            cancel_callback()

async def ws_subscribe_binance_stream(ws,*streams):
    id = ws_id_sequence.next()
    
    subscribe_req = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": id
    }
    subscribe_req_str = json.dumps(subscribe_req)

    await ws.send(subscribe_req_str)

class BinanceWSReceiver:
    SPOT_API_WS_URI = "wss://stream.binance.com:9443/ws"
    USDM_FUTURES_API_WS_URI = "wss://fstream.binance.com/ws"

    def __init__(self,uri):
        self._uri = uri
        self._book_order_subscriptions = list()
        self._kline_subscriptions = list()
        self._trade_subscriptions = list()
        self._active_task = None
        self._current_ws = None
        self._message_router = MessageRouter()
    
    async def _recieve_book_order_subscription_data(self,subscription,immediate_subscription = True):
        book_order_prefetcher,stream_name,symbol = subscription
        if not self._current_ws is None:
            try:
                self._message_router.add_route(lambda msg:msg.get('e') == 'depthUpdate',process_depth_update,book_order_prefetcher.ws_receiver)
                if immediate_subscription:
                    await ws_subscribe_binance_stream(self._current_ws,stream_name)
                asyncio.create_task(load_book_order(symbol,book_order_prefetcher.http_receiver,sleep_time = 5,cancel_callback = lambda:book_order_prefetcher.cancel_prefetch()))
            except ConnectionClosed:
                pass
    
    async def _receive_klines_subscription_data(self,subscription,immediate_subscription = True):
        callback,interval,symbol,stream_name = subscription
        if not self._current_ws is None:
            try:
                self._message_router.add_route(lambda msg:msg.get('e') == 'kline' and msg.get('s') == symbol and msg.get('k').get('i') == interval,process_kline_event,callback)
                if immediate_subscription:
                    await ws_subscribe_binance_stream(self._current_ws,stream_name)
            except ConnectionClosed:
                pass
    
    async def _receive_trade_subscription_data(self,subscription,immediate_subscription = True):
        callback,symbol,stream_name = subscription
        if not self._current_ws is None:
            try:
                self._message_router.add_route(lambda msg:msg.get('e') == 'trade' and msg.get('s') == symbol,process_trade_event,callback)
                if immediate_subscription:
                    await ws_subscribe_binance_stream(self._current_ws,stream_name)
            except ConnectionClosed:
                pass
    
    def subscribe_book_order(self,symbol):
        async_iterator = AsyncIterQueue()
        upper_symbol = symbol.upper()
        book_order_prefetcher = BookOrderPrefetcher(lambda x:async_iterator.send(x),upper_symbol)
        stream_name = symbol.lower() + "@depth"
        
        subscription = (book_order_prefetcher,stream_name,upper_symbol)
        self._book_order_subscriptions.append(subscription)
        if not self._active_task is None:
            asyncio.create_task(self._recieve_book_order_subscription_data(subscription))
        
        return async_iterator
    
    def subscribe_klines(self,symbol,interval):
        upper_symbol = symbol.upper()
        async_iterator = AsyncIterQueue()
        stream_name = symbol.lower() + "@kline_" + interval
        callback = lambda x:async_iterator.send(x)
        
        subscription = callback,interval,upper_symbol,stream_name
        self._kline_subscriptions.append(subscription)
        if not self._active_task is None:
            asyncio.create_task(self._receive_klines_subscription_data(subscription))
        
        return async_iterator

    def subscribe_trades(self,symbol):
        upper_symbol = symbol.upper()
        async_iterator = AsyncIterQueue()
        stream_name = symbol.lower() + "@trade"
        callback = lambda x:async_iterator.send(x)

        subscription = (callback,upper_symbol,stream_name)
        self._trade_subscriptions.append(subscription)
        if not self._active_task is None:
            asyncio.create_task(self._receive_trade_subscription_data(subscription))
        
        return async_iterator

    async def _run(self):
        try:
            self._active_task = asyncio.current_task()
            while True:
                try:
                    async with websockets.connect(self._uri) as ws:
                        self._current_ws = ws
                        subs_streams_list = list()

                        for subscription in self._book_order_subscriptions:
                            await self._recieve_book_order_subscription_data(subscription, immediate_subscription = False)
                            subs_streams_list.append(subscription[1])
                        
                        for subscription in self._kline_subscriptions:
                            await self._receive_klines_subscription_data(subscription, immediate_subscription = False)
                            subs_streams_list.append(subscription[3])
                        
                        for subscription in self._trade_subscriptions:
                            await self._receive_trade_subscription_data(subscription, immediate_subscription = False)
                            subs_streams_list.append(subscription[2])
                        
                        await ws_subscribe_binance_stream(self._current_ws,*subs_streams_list)
                        
                        while True:
                            msg = await ws.recv()
                            parse_success = False
                            jmsg = None
                            try:
                                jmsg = json.loads(msg)
                                parse_success = True
                            except json.JSONDecodeError:
                                print("Unable to decode message - " + msg)
                            
                            if parse_success:
                                try:
                                    self._message_router.put_message(jmsg)
                                except KeyboardInterrupt:
                                    raise
                                except asyncio.CancelledError:
                                    raise
                                except:
                                    print("Message processing error")
                                    traceback.print_exc()
                except KeyboardInterrupt:
                    raise
                except asyncio.CancelledError:
                    raise
                except:
                    self._current_ws = None
                    print("Connection error!")
                    traceback.print_exc()
                    print()
                    print("Waiting minute for reconnect...")
                    await asyncio.sleep(60)

                self._current_ws = None
        finally:
            self._active_task = None
    
    def start(self):
        if self._active_task is None:
            asyncio.create_task(self._run())
    
    def stop(self):
        task = self._active_task
        if not task is None:
            task.cancel()

class BinanceWSReceiverProvider:
    _receiver_uri_mapping = {
        "spot": BinanceWSReceiver.SPOT_API_WS_URI,
        "usdm_futures": BinanceWSReceiver.USDM_FUTURES_API_WS_URI
    }

    def __init__(self):
        self._receivers = dict()
        self._lock = threading.Lock()
    
    def get(self,api_type):
        receiver = None
        with self._lock:
            if not api_type in self._receivers:
                uri = self._receiver_uri_mapping[api_type]
                receiver = BinanceWSReceiver(uri)
                self._receivers[api_type] = receiver
            else:
                receiver = self._receivers[api_type]
        
        return receiver
    
    def start(self):
        for receiver in self._receivers.values():
            receiver.start()