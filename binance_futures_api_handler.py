import gc
import queue
import threading

from common import *
from binance.client import Client
from binance import ThreadedWebsocketManager


# TODO generalize account message processor
class BinanceFuturesApiHandler(Client):
    _market_ws_client = None
    _ws_market_updates_thread = None
    _current_prices = {}
    _on_price_update_callbacks = {"update_finished": {}}  # callback_id:[callback, self]

    _market_message_queue = queue.Queue()
    _assets_precision = {}
    _symbols_data = {}
    _market_ws_task = None
    _logger = get_logger('futures_general')
    _should_skip_price_update_msg = False  # for debug
    _is_shutdown = False

    def __init__(self, username, public_key=None, private_key=None, logger=None, is_hedge=False):
        super().__init__(api_key=public_key, api_secret=private_key)
        self.logger = get_logger('default', public_key) if logger is None else logger

        self.username = username
        self.account_type = "futures"
        self.max_retry_counter = 5
        self.account_ws_clients = None
        self.is_hedge = is_hedge
        self.filter_clean_mutex = threading.Lock()

        try:
            self.futures_coin_change_position_mode(dualSidePosition=self.is_hedge)
            self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"Change: hedge enabled {self.is_hedge}"))
        except Exception as e:
            if "-4059" in str(e):
                self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                                 f"No need to change hedge position side, hadge enabled: {self.is_hedge}"))
            else:
                handle_exception(self.logger, e)

        self.ws_account_updates_callbacks = {"accountUpdate": [], "leverageUpdate": [], "orderUpdate": {}}
        self.ws_account_updates_thread = threading.Thread(target=self.run_account_updates, daemon=True)

        self.last_clean_msg_filter_timestamp = time.time()
        self.account_message_filter = {}
        self.account_message_queue = queue.Queue()
        self.ws_process_tasks = {}

    @classmethod
    async def create(cls, username, public, private, logger=None, is_hedge=False):
        try:
            self = BinanceFuturesApiHandler(username=username, public_key=public, private_key=private, logger=logger, is_hedge=is_hedge)
            self.ws_process_tasks["account"] = asyncio.create_task(self.account_ws_messages_processor())

            if cls._ws_market_updates_thread is None:
                cls._ws_market_updates_thread = threading.Thread(target=BinanceFuturesApiHandler.run_market_updates, daemon=True)
                cls.load_exchange_assets_info(self)
                cls._market_ws_task = asyncio.create_task(BinanceFuturesApiHandler.market_ws_messages_processor())
            return self
        except Exception as e:
            handle_exception(get_logger('default'), e)

    async def start_ws_updates(self):
        try:
            if not type(self)._ws_market_updates_thread.is_alive():
                type(self)._ws_market_updates_thread.start()

            self.ws_account_updates_thread.start()
        except Exception as e:
            handle_exception(self.logger, e)

    @staticmethod
    def run_market_updates():
        while True:
            try:
                if BinanceFuturesApiHandler._market_ws_client is None or not BinanceFuturesApiHandler._market_ws_client.is_alive():
                    BinanceFuturesApiHandler._market_ws_client = ThreadedWebsocketManager()
                    BinanceFuturesApiHandler._market_ws_client.daemon = True
                    BinanceFuturesApiHandler._market_ws_client.start()
                    BinanceFuturesApiHandler._market_ws_client.start_all_mark_price_socket(callback=BinanceFuturesApiHandler.market_websocket_process_new_msg)
                if BinanceFuturesApiHandler._is_shutdown:
                    print("Futures Api handler run market updates shutdown")
                    BinanceFuturesApiHandler._market_ws_client.stop()
                    return

                time.sleep(5)
            except Exception as e:
                handle_exception(BinanceFuturesApiHandler._logger, e)

    @staticmethod
    def market_websocket_process_new_msg(message):
        try:
            BinanceFuturesApiHandler._market_message_queue.put(message)
        except Exception as e:
            handle_exception(BinanceFuturesApiHandler._logger, e)

    @staticmethod
    def get_new_market_websocket_message():
        try:
            return BinanceFuturesApiHandler._market_message_queue.get(timeout=10)
        except queue.Empty:
            return None
        except Exception as e:
            handle_exception(BinanceFuturesApiHandler._logger, e)

    @staticmethod #Should every instance have it own message queue in case it is ran in separate thread? So we have to connect queue filler to each of them?
    async def market_ws_messages_processor():
        """
           { ... ADAUSDT:{callback_id:[callback, class_ref]} }
           pass float last price
        """
        try:
            loop = asyncio.get_running_loop()
            running_tasks = []
            while True:
                message = await loop.run_in_executor(None, BinanceFuturesApiHandler.get_new_market_websocket_message)
                if message is None:
                    if BinanceFuturesApiHandler._is_shutdown:
                        return
                    else:
                        continue
                running_tasks = clean_finished_tasks(running_tasks)
                msg_data = message.get("data")
                if msg_data is None or not isinstance(msg_data, list) or msg_data[0].get("e") != "markPriceUpdate":
                    BinanceFuturesApiHandler._logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Got not futures ticker update message: {message}"))
                    continue

                if BinanceFuturesApiHandler._should_skip_price_update_msg:
                    continue

                for symbol_data in msg_data:
                    symbol = symbol_data["s"]
                    last_price = float(symbol_data["p"])
                    BinanceFuturesApiHandler._current_prices[symbol] = last_price

                    symbol_price_update_callbacks = BinanceFuturesApiHandler._on_price_update_callbacks.get(symbol)

                    if symbol_price_update_callbacks is not None:
                        for key in symbol_price_update_callbacks:
                            callback_data = symbol_price_update_callbacks[key]
                            func_to_call = callback_data[0]
                            task = asyncio.create_task(func_to_call(callback_data[1], last_price))
                            running_tasks.append(task)

                price_update_over_callbacks = BinanceFuturesApiHandler._on_price_update_callbacks["update_finished"]
                if price_update_over_callbacks is not None:
                    for key in price_update_over_callbacks:
                        callback_data = price_update_over_callbacks[key]
                        func_to_call = callback_data[0]
                        task = asyncio.create_task(func_to_call(callback_data[1], BinanceFuturesApiHandler._current_prices))
                        running_tasks.append(task)
        except Exception as e:
            handle_exception(BinanceFuturesApiHandler._logger, e)

    def load_exchange_assets_info(self):
        """
        Fills dicts of
            symbols_data:dict
            assets_precision:dict

        symbols_data include:
            lot_size_step;
            max_lot_size;
            min_lot_size;
            price_tick;
            min_notional;
            base;
            quote;

            all in str type
            provides opportunity to get base and quote of symbol
            min_notional is min quote in order: price * quantity >= minNotional
            min_lot_size is min base asset amount allowed in order

        assets_precision is dict include
            asset:significant_digits_num  - example: ETH:8 (valuable digits)
        """
        try:
            symbols_information = self.futures_exchange_info()
            assets_precision = {}
            symbols_data = {}
            symbols_information = symbols_information["symbols"]

            for symbol_data in symbols_information:
                symbol = symbol_data['symbol']
                base_asset = symbol_data['baseAsset']
                base_asset_precision = symbol_data['baseAssetPrecision']
                quote_asset = symbol_data['quoteAsset']
                quote_asset_precision = symbol_data['quotePrecision']

                if assets_precision.get(base_asset) is None:
                    assets_precision[base_asset] = base_asset_precision

                if assets_precision.get(quote_asset) is None:
                    assets_precision[quote_asset] = quote_asset_precision

                data = {}
                filter_information = symbol_data["filters"]

                for filter in filter_information:
                    if filter["filterType"] == "LOT_SIZE":
                        data['lot_size_step'] = filter["stepSize"]
                        data['min_lot_size'] = filter["minQty"]  # min base in order
                        data['max_lot_size'] = filter["maxQty"]
                    elif filter["filterType"] == "PRICE_FILTER":
                        data['price_tick'] = filter["tickSize"]
                    elif filter["filterType"] == "MIN_NOTIONAL":
                        data['min_notional'] = filter["notional"]  # min quote in order: price * quantity >= minNotional

                data['base'] = base_asset
                data['quote'] = quote_asset

                symbols_data[symbol] = data
            type(self)._symbols_data = symbols_data
            type(self)._assets_precision = assets_precision
        except Exception as e:
            handle_exception(self.logger, e)

    def subscribe_for_price_update(self, symbol, callback: list):
        """Sub to 'update_finished' for all symbols data"""
        try:
            callback_id = generate_id(10)
            symbol = symbol.upper()
            callbacks_data = type(self)._on_price_update_callbacks.get(symbol)
            if callbacks_data is None:
                type(self)._on_price_update_callbacks[symbol] = {callback_id: callback}
            else:
                callbacks_data[callback_id] = callback
            return callback_id
        except Exception as e:
            handle_exception(self.logger, e)

    def unsubscribe_from_price_update(self, symbol, callback_id):
        try:
            symbol = symbol.upper()
            callbacks_data = type(self)._on_price_update_callbacks.get(symbol)
            if callbacks_data is not None:
                callback = callbacks_data.get(callback_id)
                if callback is not None:
                    del callbacks_data[callback_id]
                    if len(callbacks_data) == 0:
                        del type(self)._on_price_update_callbacks[symbol]
                else:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Unsubscribe price update - no callback for symbol {symbol} with provided id: {callback_id}, {callbacks_data}"))
            else:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Unsubscribe price update - no callbacks for symbol: {symbol}"))
        except Exception as e:
            handle_exception(self.logger, e)

    def calculate_quantity(self, symbol, quote_amount, price=None):
        try:
            if price is None:
                price = self.get_current_price(symbol)
            else:
                price = round(float(price), ROUND_PRECISION)

            base_qty = round(float(quote_amount), ROUND_PRECISION) / price
            base_qty = self.apply_amount_precision(symbol, base_qty)
            return base_qty
        except Exception as e:
            handle_exception(self.logger, e)

    def apply_price_precision(self, symbol, price):
        try:
            symbol = symbol.upper()
            data = type(self)._symbols_data.get(symbol)
            if data is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Get precision - no data for symbol: {symbol}"))
                return price

            precision = data.get('price_tick')
            if precision is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Get price precision - no data for symbol: {symbol} has no precision: {data}"))
                return price

            if float(precision) < 1:
                digits_num_after_comma_allowed = len(precision.strip("0")) - 1
                str_price = str(price)
                if "." in str_price:
                    price_digits_after_comma_num = len(str_price.split(".")[1])
                    delta = price_digits_after_comma_num - digits_num_after_comma_allowed
                    if delta > 0:
                        str_price = str_price[:-delta]

                return str_price
            else:
                return str(int(int(round(float(price) / float(precision), ROUND_PRECISION)) * round(float(precision),
                                                                                                    ROUND_PRECISION)))
        except Exception as e:
            handle_exception(self.logger, e)
            return price

    def apply_amount_precision(self, symbol, amount, is_base=True):
        try:
            symbol = symbol.upper()
            data = type(self)._symbols_data.get(symbol)
            if data is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Get amount precision - no data for symbol: {symbol}"))
                return amount

            precision = None
            if is_base:
                precision = data.get('lot_size_step')
            else:
                precision = data.get('price_tick')

            if precision is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Get precision = None amount precision - no data for symbol: {symbol} has no precision: {data}"))
                return amount

            if float(precision) < 1:
                digits_num_after_comma_allowed = len(precision.strip("0")) - 1
                str_amount = str(amount)
                if "." in str_amount:
                    amount_digits_after_comma_num = len(str_amount.split(".")[1])
                    delta = amount_digits_after_comma_num - digits_num_after_comma_allowed
                    if delta > 0:
                        str_amount = str_amount[:-delta]
                return str_amount
            else:
                return str(int(int(round(float(amount) / float(precision), ROUND_PRECISION)) * round(float(precision),
                                                                                                     ROUND_PRECISION)))
        except Exception as e:
            handle_exception(self.logger, e)
            return amount

    def get_current_price(self, symbol):
        try:
            symbol = symbol.upper()
            return type(self)._current_prices.get(symbol)
        except Exception as e:
            handle_exception(self.logger, e)

    def get_min_base_order_amount(self, symbol):
        try:
            min_lot = None
            symbol = symbol.upper()
            data = type(self)._symbols_data.get(symbol)
            if data:
                min_lot = data.get("min_lot_size")
            else:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Get min lot size - no data for symbol: {symbol}"))
            return min_lot
        except Exception as e:
            handle_exception(self.logger, e)


    def launch_account_websocket(self):
        try:
            print("Launching new futures account ws connection")
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Launching new futures account ws connection"))
            ws_manager = ThreadedWebsocketManager(api_key=self.API_KEY, api_secret=self.API_SECRET)
            ws_manager.daemon = True
            ws_manager.start()
            ws_manager.start_futures_socket(callback=self.account_websocket_process_new_msg)

            return ws_manager
        except Exception as e:
            handle_exception(self.logger, e)

    def run_account_updates(self):
        self.account_ws_clients = []
        last_dropped_ts = int(time.time())
        dropped_ws_count = 0

        for i in range(2):
            self.account_ws_clients.append(self.launch_account_websocket())

        while True:
            try:
                ws_to_drop = []
                cleaned_ws = []
                time_now = int(time.time())

                for ws in self.account_ws_clients:
                    if not ws.is_alive():
                        print("Dead client")
                        self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Dead account ws client thread"))
                        ws.stop()
                        ws_to_drop.append(ws)
                        last_dropped_ts = time_now
                    elif time_now - last_dropped_ts > 3600:
                        print("Client drop")
                        self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Dropping ws account connection - regular update"))
                        ws_to_drop.append(ws)
                        last_dropped_ts = time_now
                    elif type(self)._is_shutdown:
                        print("Drop ws due shutdown")
                        self.logger.debug(compose_log_msg(getframeinfo(currentframe()), "Drop ws due to shutdown"))
                        ws_to_drop.append(ws)
                    else:
                        cleaned_ws.append(ws)

                self.account_ws_clients = cleaned_ws
                for ws in ws_to_drop:
                    if not type(self)._is_shutdown:
                        print("Creating new account WS handler due to drop")
                        cleaned_ws.append(self.launch_account_websocket())
                    ws.stop()

                dropped_ws_count = len(ws_to_drop)
                ws_to_drop.clear()

                if type(self)._is_shutdown:
                    print("Futures Api handler run account updates shutdown")
                    return

                time.sleep(10)
                if dropped_ws_count:
                    gc.collect()
                    dropped_ws_count = 0

            except Exception as e:
                if type(self)._is_shutdown:
                    return
                handle_exception(self.logger, e)

    def account_websocket_process_new_msg(self, message):
        try:
            print(f"New msg: {message}")
            self.account_message_queue.put(message)
        except Exception as e:
            handle_exception(self.logger, e)

    def get_new_account_websocket_message(self):
        try:
            return self.account_message_queue.get(timeout=10)
        except queue.Empty:
            return None
        except Exception as e:
            handle_exception(self.logger, e)

    async def account_ws_messages_processor(self):
        """accountUpdate
           leverageUpdate
           orderUpdate
           """
        try:
            loop = asyncio.get_running_loop()
        except Exception as e:
            handle_exception(self.logger, e)

        running_tasks = []
        while True:
            try:
                self.logger.info(compose_log_msg(getframeinfo(currentframe()), "Wait futures WS account update msg"))

                message = await loop.run_in_executor(None, BinanceFuturesApiHandler.get_new_account_websocket_message, self)
                if message is None:
                    if not BinanceFuturesApiHandler._is_shutdown:
                        continue
                    else:
                        return

                transaction_timestamp = message.get("T") #transaction time - time that the data (e.g. account, order related) got updated
                event_timestamp = message.get("E") #event time - represents the time a certain data was pushed out from the server

                try:
                    del message["E"]
                except:
                    self.logger.error(f"(Not a real error) No transaction timestamp for msg {message}")

                event_type = message.get("e")
                parsed_msg = message
                if event_type == "ORDER_TRADE_UPDATE":
                    order_info = message.get("o")
                    parsed_msg = {
                        "event_type": event_type,
                        "timestamp": transaction_timestamp,
                        "symbol": order_info.get("s"),
                        "exchange_order_id": order_info.get("i"),
                        "client_order_id": order_info.get("c"),
                        "side": order_info.get("S"),
                        "position_side": order_info.get("ps"),
                        "order_type": order_info.get("o"),
                        "quantity": order_info.get("q"),
                        "action_price": order_info.get("p"),
                        "trigger_price": order_info.get("sp"),
                        "avg_price": order_info.get("ap"),
                        "order_status": order_info.get("X"),
                        "total_filled": order_info.get("z"),
                        "is_repay": order_info.get("R")  # absent for margin and in hedge mode
                    }

                if not self.filter_account_msg(parsed_msg):
                    continue

                #did not append b4 cause of get_order_info - there is no such info in it
                if event_type == "ORDER_TRADE_UPDATE":
                    parsed_msg["commission_asset"] = order_info.get("N")
                    parsed_msg["commission"] = order_info.get("n")

                self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Got new WS futures upd: {message}"))

                running_tasks = clean_finished_tasks(running_tasks)

                if event_type is None:
                    self.logger.error(f"Message had NO event type field: {message}")

                elif event_type == "ACCOUNT_UPDATE":
                    callbacks = self.ws_account_updates_callbacks.get("accountUpdate")
                    if len(callbacks) != 0:
                        for elem in callbacks:
                            func_to_call = elem[0]
                            task = asyncio.create_task(func_to_call(elem[1], message))
                            running_tasks.append(task)
                    else:
                        self.logger.debug(
                            compose_log_msg(getframeinfo(currentframe()), "No callbacks on accountUpdate"))

                elif event_type == "ORDER_TRADE_UPDATE":
                    order_update_callbacks = self.ws_account_updates_callbacks.get("orderUpdate")
                    order_id = parsed_msg.get("client_order_id")

                    if len(order_update_callbacks) != 0:
                        callback_data = order_update_callbacks.get(order_id)
                        if callback_data is None:
                            self.logger.warning(compose_log_msg(getframeinfo(currentframe()),
                                                                f"No handler for order update: {message}"))
                        else:
                            func_to_call = callback_data[0]
                            task = asyncio.create_task(func_to_call(callback_data[1], parsed_msg))
                            running_tasks.append(task)
                    else:
                        self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                          f"No callbacks on 'executionReport' for order {order_id}"))

                elif event_type == "ACCOUNT_CONFIG_UPDATE":
                    leverage_update_callbacks = self.ws_account_updates_callbacks.get("leverageUpdate")

                    for callback in leverage_update_callbacks:
                        func_to_call = callback[0]
                        task = asyncio.create_task(func_to_call(callback[1], message))
                        running_tasks.append(task)
            except Exception as e:
                if type(self)._is_shutdown:
                    return
                handle_exception(self.logger, e)

    def clean_msg_filter(self):
        try:
            successfully_acquired = self.filter_clean_mutex.acquire(False)
            if successfully_acquired:
                try:
                    time_now = time.time()

                    if len(self.account_message_filter) > 5000 or time_now - self.last_clean_msg_filter_timestamp > 300:  # 5 min
                        self.last_clean_msg_filter_timestamp = time_now
                        keys = list(self.account_message_filter.keys())
                        for key in keys:
                            if time_now - self.account_message_filter[key] > 500:
                                del self.account_message_filter[key]
                finally:
                    self.filter_clean_mutex.release()
        except Exception as e:
            handle_exception(self.logger, e)

    def filter_account_msg(self, parsed_msg):
        try:
            self.clean_msg_filter()
            msg_hash = hash(str(parsed_msg))
            message_present = self.account_message_filter.get(msg_hash)
            if message_present:  # 800 mil sec
                print("Drop repeated msg")
                if int(time.time()*1000) - int(message_present) < 0.8:
                    self.logger.warning(compose_log_msg(getframeinfo(currentframe()),
                                                        f"Got similar upd message for order {parsed_msg.get('client_order_id')} since {time.time()*1000 - message_present} seconds after 1 such msg arrived : {parsed_msg}"))
                return False

            timestamp = parsed_msg.get("timestamp") if parsed_msg.get("timestamp") else parsed_msg.get("T")
            self.account_message_filter[msg_hash] = timestamp
            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def subscribe_on_order_update(self, order_id, callback: list):
        try:
            callbacks_data = self.ws_account_updates_callbacks["orderUpdate"]
            callbacks_data[order_id] = callback
            self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"Order with id {order_id} subbed for updates"))
        except Exception as e:
            handle_exception(self.logger, e)

    def unsubscribe_from_order_update(self, order_id):
        try:
            callbacks_data = self.ws_account_updates_callbacks["orderUpdate"].get(order_id)
            if callbacks_data is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Unsubscribe order update - no callbacks for order: {order_id}"))
                return

            del self.ws_account_updates_callbacks["orderUpdate"][order_id]
            self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"Order with id {order_id} succesfully unsubed from updates"))
        except Exception as e:
            handle_exception(self.logger, e)



    def place_market_order(self, symbol: str, side: str,
                           base_amount=None, quote_amount=None,
                           order_id: str = None, is_repay: bool = False,
                           should_retry_on_failure=True, should_filter_result=True):
        try:
            symbol = symbol.upper()
            side = side.upper()
            reduce_only = "false"

            if side != "SELL" and side != "BUY":
                return None, f"Invalid side: {side}; Allowed: SELL | BUY"

            if is_repay:
                reduce_only = "true"

            position_side = "SHORT" if (side == "BUY" and is_repay) or (side == "SELL" and not is_repay) else "LONG"

            order_parameters = {

                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "positionSide": position_side if self.is_hedge else "BOTH",
            }

            if not self.is_hedge:
                order_parameters["reduceOnly"] = reduce_only

            if base_amount is not None:
                order_parameters["quantity"] = self.apply_amount_precision(symbol, base_amount)
            elif quote_amount is not None:
                order_parameters["quantity"] = self.calculate_quantity(symbol, quote_amount)
            else:
                return None, "Both Base and Quote order amount are None"

            if order_id is not None:
                order_parameters["newClientOrderId"] = order_id

            retry_counter = -1
            while retry_counter < self.max_retry_counter:
                retry_counter += 1
                try:
                    result = self.futures_create_order(**order_parameters)
                except Exception as e:
                    handle_exception(self.logger, e)
                    result = str(e)

                if should_retry_on_failure:
                    if "Connection aborted" in result or "-1021" in result:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                       f"Order {order_parameters['newClientOrderId']} failed to be placed: {result}; Retry num: {retry_counter}"))
                        continue

                result_status = result.get("status") if isinstance(result, dict) else None
                if result_status is None:
                    return result, None

                parsed_msg = self.parse_order_update_msg(result)
                if should_filter_result:
                    filtered_result = self.filter_account_msg(parsed_msg)
                    return parsed_msg, filtered_result
                else:
                    return parsed_msg, None
        except Exception as e:
            handle_exception(self.logger, e)
            return str(e), None

    def place_limit_order(self, symbol: str, side: str, action_price,
                          base_amount=None, quote_amount=None,
                          order_id: str = None, time_in_force: str = "GTC",
                          is_repay: bool = False, should_retry_on_failure=True,
                          should_filter_result=True):
        try:
            symbol = symbol.upper()
            side = side.upper()
            reduce_only = "false"

            if side != "SELL" and side != "BUY":
                return None, f"Invalid side: {side}; Allowed: SELL | BUY"

            if is_repay:
                reduce_only = "true"

            position_side = "SHORT" if (side == "BUY" and is_repay) or (side == "SELL" and not is_repay) else "LONG"
            order_parameters = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side if self.is_hedge else "BOTH",
                "price": self.apply_price_precision(symbol, action_price),
                "type": "LIMIT",
                "timeInForce": time_in_force
            }

            if not self.is_hedge:
                order_parameters["reduceOnly"] = reduce_only

            if base_amount is not None:
                order_parameters["quantity"] = self.apply_amount_precision(symbol, base_amount)
            elif quote_amount is not None:
                order_parameters["quantity"] = self.calculate_quantity(symbol, quote_amount, action_price)
            else:
                return None, "Both Base and Quote order amount are None"

            if order_id is not None:
                order_parameters["newClientOrderId"] = order_id

            retry_counter = -1
            while retry_counter < self.max_retry_counter:
                retry_counter += 1
                try:
                    result = self.futures_create_order(**order_parameters)
                except Exception as e:
                    handle_exception(self.logger, e)
                    result = str(e)

                if should_retry_on_failure:
                    if "-4015" in result: #Client order id is not valid - probably order with such ID exists
                        args = {"symbol": symbol, "client_order_id": order_id, "apply_msg_filter": False}
                        result = self.get_order_info(**args)

                    if "-4014" in result:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Order {order_parameters['newClientOrderId']} failed to be placed: {result}; Retry num: {retry_counter}"))
                        current_price = order_parameters["price"]
                        if "." in current_price:
                            if current_price[-2] != ".":
                                order_parameters["price"] = current_price[:-1]
                            else:
                                order_parameters["price"] = current_price[:-2]
                        continue
                    elif "Connection aborted" in result or "-1021" in result:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Order {order_parameters['newClientOrderId']} failed to be placed: {result}; Retry num: {retry_counter}"))
                        continue

                result_status = result.get("status") if isinstance(result, dict) else None
                if result_status is None:
                    return result, None

                parsed_msg = self.parse_order_update_msg(result)
                if should_filter_result:
                    filtered_result = self.filter_account_msg(parsed_msg)
                    return parsed_msg, filtered_result
                else:
                    return parsed_msg, None

        except Exception as e:
            handle_exception(self.logger, e)
            return str(e), None

    def place_stop_order(self, symbol: str, side: str, trigger_price, action_price=None,
                         base_amount=None, quote_amount=None,
                         order_id: str = None, time_in_force: str = "GTC",
                         is_repay: bool = False, should_retry_on_failure=True,
                         should_filter_result=True):
        try:
            symbol = symbol.upper()
            side = side.upper()
            reduce_only = "false"

            if side != "SELL" and side != "BUY":
                return None, f"Invalid side: {side}; Allowed: SELL | BUY"

            if is_repay:
                reduce_only = "true"

            position_side = "SHORT" if (side == "BUY" and is_repay) or (side == "SELL" and not is_repay) else "LONG"
            order_parameters = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side if self.is_hedge else "BOTH",
                "stopPrice": self.apply_price_precision(symbol, trigger_price),
                "type": "STOP_MARKET",
                "timeInForce": time_in_force
            }

            if not self.is_hedge:
                order_parameters["reduceOnly"] = reduce_only

            if action_price is not None:
                order_parameters["type"] = "STOP"
                order_parameters["price"] = self.apply_price_precision(symbol, action_price)

            if base_amount is not None:
                order_parameters["quantity"] = self.apply_amount_precision(symbol, base_amount)
            elif quote_amount is not None:
                order_parameters["quantity"] = self.calculate_quantity(symbol, quote_amount, action_price)
            else:
                return None, "Both Base and Quote order amount are None"

            if order_id is not None:
                order_parameters["newClientOrderId"] = order_id

            retry_counter = -1
            while retry_counter < self.max_retry_counter:
                retry_counter += 1
                try:
                    result = self.futures_create_order(**order_parameters)
                except Exception as e:
                    handle_exception(self.logger, e)
                    result = str(e)

                if should_retry_on_failure:
                    if "-4015" in result: #Client order id is not valid - probably order with such ID exists
                        args = {"symbol": symbol, "client_order_id": order_id, "apply_msg_filter": False}
                        result = self.get_order_info(**args)

                    if "-4014" in result:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Order {order_parameters['newClientOrderId']} failed to be placed: {result}; Retry num: {retry_counter}"))
                        current_price = order_parameters["stopPrice"]
                        if "." in current_price:
                            if current_price[-2] != ".":
                                order_parameters["stopPrice"] = current_price[:-1]
                            else:
                                order_parameters["stopPrice"] = current_price[:-2]

                            if action_price is not None:
                                order_parameters["price"] = order_parameters["stopPrice"]
                        continue
                    elif "Connection aborted" in result or "-1021" in result:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Order {order_parameters['newClientOrderId']} failed to be placed: {result}; Retry num: {retry_counter}"))
                        continue

                result_status = result.get("status") if isinstance(result, dict) else None
                if result_status is None:
                    return result, None

                parsed_msg = self.parse_order_update_msg(result)
                if should_filter_result:
                    filtered_result = self.filter_account_msg(parsed_msg)
                    return parsed_msg, filtered_result
                else:
                    return parsed_msg, None
        except Exception as e:
            handle_exception(self.logger, e)
            return str(e), None

    def parse_order_update_msg(self, msg):
        try:
            status = msg.get("status") if isinstance(msg, dict) else None

            if not status:
                return msg

            parsed_msg = {
                "event_type": "ORDER_TRADE_UPDATE",
                "timestamp": msg.get("updateTime"),
                "symbol": msg.get("symbol"),
                "exchange_order_id": msg.get("orderId"),
                "client_order_id": msg.get("clientOrderId"),
                "side": msg.get("side"),
                "position_side": msg.get("positionSide"),
                "order_type": msg.get("origType"),
                "quantity": msg.get("origQty"),
                "action_price": msg.get("price"),
                "trigger_price": msg.get("stopPrice"),
                "avg_price": msg.get("avgPrice"),
                "order_status": status,
                "total_filled": msg.get("executedQty"),
                "is_repay": msg.get("reduceOnly")  # absent for margin and hedge mode
            }
            return parsed_msg
        except Exception as e:
            handle_exception(self.logger, e)

    def get_order_info(self, symbol: str, client_order_id: str, apply_msg_filter=False):
        try:
            try:
                result = self.futures_get_order(symbol=symbol, origClientOrderId=client_order_id)
            except Exception as e:
                return str(e)

            parsed_msg = self.parse_order_update_msg(result)

            if apply_msg_filter:
                if not self.filter_account_msg(parsed_msg):
                    self.logger.warning(compose_log_msg(getframeinfo(currentframe()),
                                                        f"Order {parsed_msg['client_order_id']} already has last update received"))
                    return None
            else:
                return parsed_msg
        except Exception as e:
            handle_exception(self.logger, e)

    def cancel_running_order(self, symbol, order_id, should_apply_filter=True):
        try:
            response = self.futures_cancel_order(symbol=symbol, origClientOrderId=order_id)
            response_status = response.get("status") if isinstance(response, dict) else None
            if response_status is None:
                return response, None

            parsed_msg = self.parse_order_update_msg(response)
            if should_apply_filter:
                filtered_result = self.filter_account_msg(parsed_msg)
                return parsed_msg, filtered_result
            else:
                return parsed_msg, None
        except Exception as e:
            handle_exception(self.logger, e)
            return e, None

    def cancel_all_active_orders(self, symbol=None):
        try:
            if symbol is None:
                opened_orders = self.futures_get_open_orders()
                open_order_symbols = set()
                for order in opened_orders:
                    open_order_symbols.add(order.get("symbol"))

                for symbol in open_order_symbols:
                    self.futures_cancel_all_open_orders(symbol=symbol)
            else:
                self.futures_cancel_all_open_orders(symbol=symbol)
        except Exception as e:
            handle_exception(self.logger, e)

    def close_all_positions(self, symbol=None):
        try:
            current_positions = self.futures_account()
            positions = current_positions.get("positions")
            if positions is None:
                print(f"Can't load positions for account {self.API_KEY}")
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                              f"Can't load positions for account {self.API_KEY}"))
            for position in positions:
                pos_symbol = position.get("symbol")
                if symbol is not None and pos_symbol != symbol:
                    continue
                position_side = position.get("positionSide")
                position_amount = float(position.get("positionAmt"))
                try:
                    min_base = float(self.get_min_base_order_amount(pos_symbol))
                except TypeError:
                    self.load_exchange_assets_info()
                    min_base = float(self.get_min_base_order_amount(pos_symbol))

                if abs(position_amount) >= min_base:
                    side = "SELL"
                    if position_side == "SHORT":
                        side = "BUY"

                    if position_side == "BOTH" and position_amount < 0:
                        side = "BUY"

                    print(f"Market closing {pos_symbol} {position_side} position of {position_amount} of account {self.API_KEY}")
                    self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Market closing {pos_symbol} {position_side} position of {position_amount} of account {self.API_KEY}"))

                    self.place_market_order(symbol=pos_symbol, side=side, base_amount=abs(position_amount), is_repay=True)
        except Exception as e:
            handle_exception(self.logger, e)

