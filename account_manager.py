import json
import queue

from common import *
from orders_logic.orders_common import final_order_status
from binance_futures_api_handler import BinanceFuturesApiHandler

from orders_logic.trailing_stop_order import TrailingStopOrder
from orders_logic.combined_order import CombinedOrder
from orders_logic.custom_oco_order import CustomOcoOrder
from orders_logic.basic_orders import MarketOrder, LimitOrder, StopLimitOrder, StopMarketOrder
from input_cmd_validator import InputValidator

# TODO - if new symbol added at the moment reboot is needed
class accountManager:
    def __init__(self, username, public_key, private_key):
        self.username = username
        self.public_key = public_key
        self.__private_key = private_key
        self.loop = None
        self.out_ws_queue = queue.Queue()

        #TODO make separate loggers for each class
        self.tasks_to_be_awaited_to_finish = []
        self.logger = get_logger('root', self.public_key)

        self.validator = InputValidator(self.logger)

        self.futures_logger = get_logger("futures", self.public_key)
        self.futures_is_hedge = False
        self.futures_default_leverage = 10
        self.futures_active_orders = {}
        self.futures_balance_state = {}
        self.futures_positions_state = {}
        self.futures_api_handler = None

        self.last_received_cmd = "No signals since OMS start"
        self.last_cmd_timestamp = time.time()

        self.manual_balance_update_coming = [False, []]  # TODO add command for that

    def __del__(self):
        pass

    @classmethod
    async def create(cls, username, public, private):
        try:

            self = accountManager(username=username, public_key=public, private_key=private)
            self.loop = asyncio.get_event_loop()

            self.futures_api_handler = await BinanceFuturesApiHandler.create(username=self.username,
                                                                             public=self.public_key,
                                                                             private=self.__private_key,
                                                                             logger=self.futures_logger,
                                                                             is_hedge=self.futures_is_hedge)

            # self.position_monitor = asyncio.create_task(self.monitor_positions())
            self.futures_api_handler.ws_account_updates_callbacks["accountUpdate"].append(
                [accountManager.process_futures_account_update, self])
            self.futures_api_handler.ws_account_updates_callbacks["leverageUpdate"].append(
                [accountManager.process_futures_symbol_leverage_update, self])

            await self.futures_api_handler.start_ws_updates()
            await self.update_futures_account_state()

            return self
        except Exception as e:
            handle_exception(get_logger("default"), e)

    async def monitor_positions(self):
        try:
            futures_set_diff = None
            futures_prev_set_diff = None

            last_warning_fired_ts = time.time() - 1800

            while True:
                try:
                    time_now = time.time()
                    if time_now - self.last_cmd_timestamp > 3600*4 and time_now - last_warning_fired_ts > 1800:
                        last_warning_fired_ts = time_now
                        self.logger.warning(f"{int((time.time() - self.last_cmd_timestamp)) / 60 } minutes passed since last cmd received: {self.last_received_cmd}")

                    futures_open_orders = await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.futures_get_open_orders))
                    futures_open_orders_symbols = set()
                    for order in futures_open_orders:
                        futures_open_orders_symbols.add(order["symbol"])

                    positions_info = await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.futures_position_information)) #built-in function
                    futures_pos_symbols = set()
                    for pos in positions_info:
                        if float(pos["positionAmt"]) != 0:
                            futures_pos_symbols.add(pos["symbol"])

                    futures_set_diff = futures_pos_symbols.difference(futures_open_orders_symbols)
                    if futures_prev_set_diff is not None:
                        intersection = futures_set_diff.intersection(futures_prev_set_diff)
                        if len(intersection) > 0:
                            for symbol in intersection:
                                time_now = time.time()
                                futures_orders_history = await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.futures_get_all_orders, symbol=symbol, limit=2))
                                if futures_orders_history[-1]["status"] in final_order_status and \
                                        int(futures_orders_history[-1]["updateTime"]) + 60 < time_now*1000:
                                    self.logger.warning(f"Futures Failed positions:{intersection}")
                                    await self.cancel_orders(symbol, "FUTURES")
                                    await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.cancel_all_active_orders, symbol=symbol))
                                    await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.close_all_positions, symbol=symbol))
                    futures_prev_set_diff = futures_set_diff

                    await asyncio.sleep(60)
                except Exception as e:
                    handle_exception(self.logger, e)

        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_orders(self, symbol, account="FUTURES"):
        try:
            if account == "FUTURES":
                orders_vault = self.futures_active_orders

            for order_id in orders_vault:
                if orders_vault[order_id].symbol == symbol:
                    await orders_vault[order_id].cancel_order()
        except Exception as e:
            handle_exception(self.logger, e)

    async def finalize_service(self):
        try:
            for order_id in self.futures_active_orders:
                await self.futures_active_orders[order_id].cancel_order()

            await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.cancel_all_active_orders))
            await self.loop.run_in_executor(None, functools.partial(self.futures_api_handler.close_all_positions))

            BinanceFuturesApiHandler._is_shutdown = True

        except Exception as e:
            handle_exception(self.logger, e)


    async def update_futures_account_state(self):
        try:
            current_account_state = None
            try:
                current_account_state = self.futures_api_handler.futures_account()
            except Exception as e:
                handle_exception(self.futures_logger, e)

            if current_account_state is not None:
                for asset_data in current_account_state["assets"]:
                    self.futures_balance_state[asset_data['asset']] = {"total": float(asset_data["walletBalance"]),
                                                                       "cross": float(asset_data["crossWalletBalance"])}

                for position_data in current_account_state["positions"]:
                    self.futures_positions_state[position_data['symbol']] = {
                                                                        "positionAmt": float(position_data["positionAmt"]),
                                                                        "entryPrice": float(position_data["entryPrice"]),
                                                                        "leverage": int(position_data["leverage"])}
            else:
                self.futures_logger.error(compose_log_msg(getframeinfo(currentframe()), "update_futures_account_state failed"))
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_futures_account_update(self, update):
        try:
            print(update)
            self.futures_logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Futures account update msg: {update}"))
            update_type = update.get("e")
            if update_type is None or update_type != "ACCOUNT_UPDATE":
                self.futures_logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Got invalid update type: {update_type} from update: {update}"))
                return

            account_data = update.get("a")
            if account_data is None:
                self.futures_logger.error(
                    compose_log_msg(getframeinfo(currentframe()), f"Can't get account data from update: {update}"))
                return

            changed_balances = account_data.get("B")
            if changed_balances is None:
                self.futures_logger.info(
                    compose_log_msg(getframeinfo(currentframe()), f"Can't get balances from update: {update}"))
                return

            for balance_data in changed_balances:
                asset = balance_data.get("a")
                existed_data = self.futures_balance_state.get(asset)

                if existed_data is None:
                    existed_data = {}
                    self.futures_balance_state[asset] = existed_data

                # print("Before:", existed_data)
                existed_data["total"] = float(balance_data["wb"])
                existed_data["cross"] = float(balance_data["cw"])
                self.futures_logger.info(compose_log_msg(getframeinfo(currentframe()),
                                                 f"Futures balance update for {asset}: {existed_data}"))
                # print("After update:", existed_data)

            changed_positions = account_data.get("P")
            if changed_positions is None:
                self.futures_logger.info(
                    compose_log_msg(getframeinfo(currentframe()), f"Can't get positions data from update: {update}"))
                return

            for position_data in changed_positions:
                symbol = position_data.get("s")
                existed_data = self.futures_positions_state.get(symbol)

                if existed_data is None:
                    self.futures_logger.error(
                        compose_log_msg(getframeinfo(currentframe()),
                                        f"Had no position data for symbol: {symbol}; update: {update}"))
                    existed_data = {}
                    self.futures_positions_state[symbol] = existed_data

                print("Before:", existed_data)
                existed_data["positionAmt"] = float(position_data["pa"])
                existed_data["entryPrice"] = float(position_data["ep"])
                print("After update:", existed_data)

        except Exception as e:
            handle_exception(self.logger, e)

    async def process_futures_symbol_leverage_update(self, update):
        try:
            self.futures_logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Leverage update msg arrived: {update}"))
            update_type = update.get("e")
            if update_type is None or update_type != "ACCOUNT_CONFIG_UPDATE":
                self.futures_logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"Got invalid update type: {update_type} from update: {update}"))
                return

            leverage_update_data = update.get("ac")
            if leverage_update_data is None:
                self.futures_logger.info(
                    compose_log_msg(getframeinfo(currentframe()), f"No leverage update data from update: {update}"))
                return

            symbol = leverage_update_data.get("s")
            existed_data = self.futures_positions_state.get(symbol)

            if existed_data is None:
                existed_data = {}
                self.futures_positions_state[symbol] = existed_data

            print("Before:", existed_data)
            existed_data["leverage"] = float(leverage_update_data["l"])
            self.futures_logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"Leverage update for symbol {symbol}: {existed_data}"))
            print("After update:", existed_data)

        except Exception as e:
            handle_exception(self.logger, e)

    async def activate_new_futures_order(self, new_order, args, order_type):
        try:
            if new_order.current_order_status == "FAILED":
                return

            new_order.callbacks["readyToDie"].append([accountManager.on_futures_order_over, self])
            self.futures_active_orders[new_order.order_id] = new_order
            task = asyncio.create_task(new_order.place_order())
            self.tasks_to_be_awaited_to_finish.append(task)
            self.futures_logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"New {order_type} order arrived: {args}"))
        except Exception as e:
            handle_exception(self.logger, e)

    async def on_futures_order_over(self, order_id, tasks):
        try:
            self.tasks_to_be_awaited_to_finish = clean_finished_tasks(self.tasks_to_be_awaited_to_finish)
            self.tasks_to_be_awaited_to_finish += tasks
            finished_order = self.futures_active_orders.get(order_id)
            self.futures_logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Finalizing futures order {order_id}"))
            if finished_order is None:
                self.futures_logger.critical(compose_log_msg(getframeinfo(currentframe()),
                                                     f"Futures order with id {order_id} is absent in orders dict."))
                return

            report = {"order_id": order_id,
                      "symbol": finished_order.symbol,
                      "type": "DEAL_REPORT",
                      "status": finished_order.current_order_status}
            self.out_ws_queue.put_nowait(json.dumps(report))

            order_dump = finished_order.dump_order_data()
            if order_dump is not None:
                self.futures_logger.info(order_dump)

            del self.futures_active_orders[order_id]
            order_dump["type"] = "DEAL_REPORT"
            str_report = json.dumps(order_dump).replace("\\", "")
            self.out_ws_queue.put_nowait(str_report)
            self.futures_logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Successfully finalized futures order {order_id}"))
        except Exception as e:
            handle_exception(self.logger, e)



    async def change_symbol_leverage(self, symbol, leverage):
        try:
            chg_leverage_args = {
                "symbol": symbol,
                "leverage": int(leverage)
            }
            await self.loop.run_in_executor(None,
                                       functools.partial(self.futures_api_handler.futures_change_leverage,
                                                         **chg_leverage_args))
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_price_update_cmd(self, command): #debug CMD update price
        try:
            order_id = command.get("order_id")
            account_type = command.get("account_type")
            price = command.get("price")
            order_instance = None

            if account_type == "FUTURES":
                order_instance = self.futures_active_orders.get(order_id)

            if order_instance is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Price adjust: can't find order with such id: {order_id}"))
                return

            if price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Price adjust: None price for order with such id: {order_id}"))
                return

            price = float(price)

            await order_instance.price_update_handler(price)
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_combined_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_combined_order_cmd(command):
                return

            order_type = "COMBINED"

            open_order_params = command.get("open_order_params", {})
            close_order_params = command.get("close_order_params")

            args["open_order_params"] = open_order_params
            args["close_order_params"] = close_order_params

            new_order = CombinedOrder(**args)
            await activation_func(new_order, args, order_type)
            return
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_trailing_stop_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_trailing_stop_order_cmd(command):
                return

            order_type = "TRAILING_STOP"
            del args["quote_amount"]
            required_order_params = ["stop_price_delta", "position_entrance_price", "take_price",
                                     "stop_price_delta_after_take", "price_min_step"]

            for param in required_order_params:
                param_value = command.get(param)
                args[param] = param_value

            new_order = TrailingStopOrder(**args)
            await activation_func(new_order, args, order_type)

        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_custom_oco_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_oco_order_cmd(command):
                return

            order_type = "CUSTOM_OCO"
            args["limit_price"] = command.get("limit_price")

            args["trigger_price"] = command.get("trigger_price")
            args["action_price"] = command.get("action_price")
            args["stop_type"] = command.get("stop_type")
            new_order = CustomOcoOrder(**args)
            await activation_func(new_order, args, order_type)
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_stop_market_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_stop_market_order_cmd(command):
                return

            order_type = "STOP_MARKET"
            args["trigger_price"] = command.get("trigger_price")
            new_order = StopMarketOrder(**args)
            await activation_func(new_order, args, order_type)
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_stop_limit_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_stop_limit_order_cmd(command):
                return

            order_type = "STOP_LIMIT"

            args["trigger_price"] = command.get("trigger_price")
            args["action_price"] = command.get("action_price")

            new_order = StopLimitOrder(**args)
            await activation_func(new_order, args, order_type)
            return
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_limit_order_cmd(self, args, command, activation_func):
        try:
            if not self.validator.validate_limit_order_cmd(command):
                return

            order_type = "LIMIT"
            cmd_price = command.get("action_price")
            if cmd_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without price arrived: {command}"))
                return

            args["action_price"] = cmd_price

            new_order = LimitOrder(**args)
            await activation_func(new_order, args, order_type)
            return
        except Exception as e:
            handle_exception(self.logger, e)

    #TODO update
    async def process_cancel_order_cmd(self, command):
        try:
            if not self.validator.validate_cancel_order_cmd(command):
                return

            liquidate_possible = ["TRAILING_STOP", "COMBINED"]
            order_id = command.get("order_id")
            should_liquidate_position = bool(command.get("should_liquidate"))
            account_type = command.get("account_type")

            if account_type == "FUTURES":
                order = self.futures_active_orders.get(order_id)


            if order is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Cancel order cmd: no active order with order_id specified: {command}"))
                report = {"order_id": order_id, "symbol":command.get("symbol"),
                          "type": "DEAL_REPORT", "status": "Finalizing", "err": "order_unfound"}
                self.out_ws_queue.put_nowait(json.dumps(report))
                return

            if order.order_type in liquidate_possible:
                await order.cancel_order(should_liquidate_position)
            else:
                await order.cancel_order()
        except Exception as e:
            handle_exception(self.logger, e)

        async def check_required_params_present(self, required_params, command, order_type):
            try:
                for param in required_params:
                    param_value = command.get(param)

                    if param_value is None:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"New {order_type} order cmd without {param} arrived: {command}"))
                        return None
                return True
            except Exception as e:
                handle_exception(self.logger, e)
                return False

    async def process_get_balance_cmd(self, command):
        try:
            asset = command.get("asset")
            account = command.get("account")
            total = -1

            if account == "FUTURES":
                usdt_balance = self.futures_balance_state.get(asset.upper())
                if usdt_balance is not None:
                    total = usdt_balance.get("total")

            command["balance"] = total
            self.out_ws_queue.put_nowait(json.dumps(command))
        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_order_cmd(self, command):
        try:
            if not self.validator.validate_new_order_cmd(command):
                return

            order_type = command.get("order_type")
            cmd_account = command.get("account_type")
            symbol = command.get("symbol").upper()

            if cmd_account == "FUTURES":
                api_handler = self.futures_api_handler

            args = {
                "api_handler": api_handler,
                "symbol": symbol,
                "side": command.get("side").upper(),
                "base_amount": command.get("base"),
                "quote_amount": command.get("quote"),
                "is_repay": bool(command.get("is_repay")),
                "order_id": command.get("order_id")
            }
            args["logger"] = args.get("api_handler").logger

            if cmd_account == "FUTURES":
                activate_order_func = self.activate_new_futures_order
                cmd_leverage = command.get("leverage")
                needed_symbol_leverage = self.futures_default_leverage if cmd_leverage is None else cmd_leverage
                current_symbol_leverage = None
                symbol_position_data = self.futures_positions_state.get(symbol)

                if symbol_position_data is None:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Can't get set leverage level for symbol: {symbol}"))
                else:
                    current_symbol_leverage = symbol_position_data.get("leverage")

                if needed_symbol_leverage != current_symbol_leverage:
                    await self.change_symbol_leverage(symbol, needed_symbol_leverage)

            if order_type == "MARKET":
                new_order = MarketOrder(**args)
                await activate_order_func(new_order, args, order_type)
                return

            if order_type == "LIMIT":
                await self.process_new_limit_order_cmd(args, command, activate_order_func)

            if order_type == "STOP_LIMIT":
                await self.process_new_stop_limit_order_cmd(args, command, activate_order_func)

            if order_type == "STOP_MARKET":
                await self.process_new_stop_market_order_cmd(args, command, activate_order_func)

            if order_type == "CUSTOM_OCO":
                await self.process_new_custom_oco_order_cmd(args, command, activate_order_func)

            if order_type == "TRAILING_STOP":
                await self.process_new_trailing_stop_order_cmd(args, command, activate_order_func)

            if order_type == "COMBINED":
                await self.process_new_combined_order_cmd(args, command, activate_order_func)

        except Exception as e:
            handle_exception(self.logger, e)

    async def process_new_cmd(self, command):
        try:
            if not self.validator.validate_new_cmd(command):
                print("cmd validation failed")
                return

            cmd_type = command.get("type")

            self.last_received_cmd = command
            self.last_cmd_timestamp = int(time.time())
            self.tasks_to_be_awaited_to_finish = clean_finished_tasks(self.tasks_to_be_awaited_to_finish)

            if cmd_type == "SWITCH PRICE SOURCE":
                BinanceFuturesApiHandler._should_skip_price_update_msg = bool(
                    not BinanceFuturesApiHandler._should_skip_price_update_msg)

                print(f"Manual price update: {BinanceFuturesApiHandler._should_skip_price_update_msg}")
                return

            if cmd_type == "PRICE ADJUST":
                await self.process_price_update_cmd(command)
                return

            if cmd_type.upper() == "NEW ORDER":
                await self.process_new_order_cmd(command)
                return

            if cmd_type.upper() == "GET_INFO":
                section = command.get("section")
                if section == "BALANCE":
                    await self.process_get_balance_cmd(command)
                return

            if cmd_type == "CANCEL":
                await self.process_cancel_order_cmd(command)

        except Exception as e:
            handle_exception(self.logger, e)

