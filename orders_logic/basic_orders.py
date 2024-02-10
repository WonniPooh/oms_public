from .orders_common import *

# TODO stopLimit will not fire if price immediately drops, maybe we should add closing by market price
# TODO handle commission + maybe handle what asset do we get and how much with exact order; Solution - pay commision with BNB
class Order:
    def __init__(self, api_handler, logger, order_type: str, symbol: str, side: str,
                 base_amount=None, quote_amount=None, order_id: str = None, is_repay: bool = False, parent_order_id:str = None):

        self.order_id = generate_id(25) if order_id is None else order_id
        self.binance_order_id = None
        self.symbol = symbol.upper()
        self.side = side
        self.order_type = order_type
        self.api_handler = api_handler
        self.quote_amount = quote_amount
        self.base_amount = base_amount
        self.logger = logger
        self.current_order_status = "INITIALIZED"
        self.is_placed = False
        self.is_repay = is_repay
        self.parent_order_id = parent_order_id

        self.order_placed_timestamp = None
        self.order_executed_timestamp = None

        self.logger.info(
            compose_log_msg(getframeinfo(currentframe()), (f"New {self.order_type} order with id:", self.order_id)))
        self.callbacks = {"onOrderFinalized": [], "readyToDie": []}

        self.commission_asset = None
        self.commission = None

        self.order_status_history = {time.time(): self.current_order_status}
        self.filled_amount = 0  # always in base asset units (ADA for pair ADAUSDT)

        self.async_tasks = []
        self.order_data = {
            "username": self.api_handler.username,
            "text_order_id": self.order_id,
            "parent_order_id": self.parent_order_id,
            "order_type": self.order_type,
            "account_type": self.api_handler.account_type,
            "symbol": self.symbol,
            "side": self.side,
            "quote_amount": self.quote_amount,
            "base_amount": self.base_amount,
            "is_repay": self.is_repay,
            "creation_ts": int(time.time() * 1000)
        }

    async def order_new_status(self, status: str):
        self.logger.info(f"Order {self.order_type} {self.order_id} update: {status}")
        self.current_order_status = status
        self.order_status_history[time.time()] = self.current_order_status

    def order_new_status_sync(self, status: str):
        self.current_order_status = status
        self.order_status_history[time.time()] = self.current_order_status
        self.logger.info(f"Order {self.order_type} {self.order_id} update: {status}")

    async def create_task(self, task_func, *args, **kwargs):
        try:
            await self.drop_finished_tasks()
            task = asyncio.create_task(task_func(*args, **kwargs))
            self.async_tasks.append(task)
        except Exception as e:
            handle_exception(self.logger, e)

    async def drop_finished_tasks(self):
        try:
            still_running_tasks = []
            for task in self.async_tasks:
                if not task.done():
                    still_running_tasks.append(task)
            self.async_tasks = still_running_tasks
        except Exception as e:
            handle_exception(self.logger, e)

    async def handle_cancel_order_error(self, response, updates_handler):
        try:
            if "-2011" in str(response):  # unknown order:
                args = {"symbol": self.symbol, "client_order_id": self.order_id, "apply_msg_filter": True}
                loop = asyncio.get_event_loop()
                order_result = await loop.run_in_executor(None, functools.partial(self.api_handler.get_order_info, **args))

                if order_result is None: #order was filtered - there is - should be - an update for it; what should I do now??
                    self.logger.warning(compose_log_msg(getframeinfo(currentframe()),
                                                        f"Order with id {self.order_id} failed to be canceled with unknown order error; Manual order status update was filtered; Nothing to do"))
                    return

                if "-2013" in order_result:
                    order_result = {"order_status": "FAILED", "client_order_id": self.order_id}

                await self.create_task(updates_handler, order_result)
        except Exception as e:
            handle_exception(self.logger, e)

    def construct_status_history_dump(self):
        try:
            output = []
            for key in self.order_status_history:
                output.append({str(key): self.order_status_history[key]})
            return output
        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = {}
            output["symbol"] = self.symbol
            output["side"] = self.side
            output["order_id"] = self.order_id
            output["order_type"] = self.order_type
            output["base_amount"] = self.base_amount
            output["quote_amount"] = self.quote_amount
            output["filled_amount"] = self.filled_amount

            output["commission_asset"] = self.commission_asset
            output["commission_payed"] = self.commission

            output["placed_timestamp"] = self.order_placed_timestamp
            output["finished_timestamp"] = self.order_executed_timestamp
            output["status_history"] = self.construct_status_history_dump()
            output["is_repay"] = str(self.is_repay)

            return output
        except Exception as e:
            handle_exception(self.logger, e)


class MarketOrder(Order):
    def __init__(self, api_handler, logger, symbol: str, side: str,
                 base_amount=None, quote_amount=None, order_id: str = None,
                 is_repay: bool = False, parent_order_id: str = None):
        super(MarketOrder, self).__init__(api_handler, logger, "MARKET", symbol, side,
                                          base_amount, quote_amount, order_id, is_repay, parent_order_id)

        self.average_price = None

    async def place_order(self):
        """Using provided api handler place order on exchange
           Either quote_amount or base amount should be provided"""
        try:
            self.api_handler.subscribe_on_order_update(self.order_id, [MarketOrder.updates_handler, self])

            arguments = {
                "symbol": self.symbol,
                "side": self.side,
                "quote_amount": self.quote_amount,
                "base_amount": self.base_amount,
                "order_id": self.order_id,
                "is_repay": self.is_repay
            }

            loop = asyncio.get_event_loop()
            response, is_filtered = await loop.run_in_executor(None, functools.partial(self.api_handler.place_market_order, **arguments))

            status = response.get("order_status") if isinstance(response, dict) else None
            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}

            if status is None:
                upd_msg["order_status"] = "FAILED"  # TODO make order finalization
                error_msg = f"Market order {self.order_id} failed to be placed: {response}"
                self.logger.error(compose_log_msg(getframeinfo(currentframe()), error_msg))
                is_filtered = False
            if not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)

            return upd_msg
        except Exception as e:
            handle_exception(self.logger, e)

    async def updates_handler(self, update_msg):
        try:
            order_id = update_msg.get("client_order_id")

            if order_id != self.order_id:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Received update with order_id {order_id} while assigned was {self.order_id}"))
                return

            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Market order with id {self.order_id} received update message: {update_msg}"))
            update_order_status = update_msg.get("order_status")

            if self.current_order_status == "FILLED": #"Partially FILLED arrived after FILLED
                return

            await self.order_new_status(update_order_status)

            if self.binance_order_id is None:
                self.binance_order_id = update_msg.get("exchange_order_id")

            if update_order_status in final_order_status:
                self.filled_amount = update_msg.get("total_filled")
                self.commission = update_msg.get("commission")
                self.commission_asset = update_msg.get("commission_asset")

                self.order_executed_timestamp = update_msg.get("timestamp")

                self.average_price = update_msg.get("avg_price")
                self.api_handler.unsubscribe_from_order_update(self.order_id)

                loop = asyncio.get_event_loop()

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    await self.create_task(callback[0], callback[1])

                final_callback = self.callbacks.get("readyToDie")

                if len(final_callback) == 1:
                    final_callback = final_callback[0]
                    await self.create_task(final_callback[0], final_callback[1], self.order_id,
                                           self.async_tasks)  # should not switch context b4 append happens
                else:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Final callback for order {self.order_id} should be always only 1 while it is {len(final_callback)}"))
        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self):
        self.logger.warning(compose_log_msg(getframeinfo(currentframe()), "Tried to cancel market order, nothing to do"))

    def dump_order_data(self):
        try:
            output = super().dump_order_data()
            output["avg_price"] = self.average_price
            return output
        except Exception as e:
            handle_exception(self.logger, e)


class LimitOrder(Order):
    def __init__(self, api_handler, logger, symbol: str, side: str, action_price,
                 base_amount=None, quote_amount=None, order_id=None, is_repay: bool = False, parent_order_id: str = None):
        super(LimitOrder, self).__init__(api_handler, logger, "LIMIT", symbol, side,
                                         base_amount, quote_amount, order_id, is_repay, parent_order_id)

        self.action_price = action_price
        self.callbacks["onOrderPlaced"] = []
        self.order_data["action_price"] = action_price

    async def place_order(self):
        try:
            self.api_handler.subscribe_on_order_update(self.order_id, [LimitOrder.updates_handler, self])

            arguments = {
                "symbol": self.symbol,
                "side": self.side,
                "action_price": self.action_price,
                "quote_amount": self.quote_amount,
                "base_amount": self.base_amount,
                "order_id": self.order_id,
                "is_repay": self.is_repay
            }

            loop = asyncio.get_event_loop()
            response, is_filtered = await loop.run_in_executor(None,
                                                               functools.partial(self.api_handler.place_limit_order, **arguments))
            status = response.get("order_status") if isinstance(response, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Limit order {self.order_id} failed to be placed: {response}"
                self.logger.error(compose_log_msg(getframeinfo(currentframe()), error_msg))
                is_filtered = False
            if not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)
            return upd_msg
        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self):
        try:
            loop = asyncio.get_event_loop()
            response, is_filtered = await loop.run_in_executor(None, functools.partial(
                                                    self.api_handler.cancel_running_order, self.symbol, self.order_id))
            status = response.get("order_status") if isinstance(response, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Limit order {self.order_id} failed to be canceled: {response}"
                self.logger.error(error_msg)
                await self.handle_cancel_order_error(response, self.updates_handler)

            if is_filtered is not None and not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)

        except Exception as e:
            handle_exception(self.logger, e)

    async def updates_handler(self, update_msg):
        try:
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Limit order {self.order_id} update: {update_msg}"))
            update_order_status = update_msg.get("order_status")

            if update_order_status is None:
                self.logger.error(f"Received order update with no status: {update_msg}")
                return

            order_id = update_msg.get("client_order_id")
            if order_id != self.order_id:
                self.logger.error(f"Received update with order_id {order_id} while assigned was {self.order_id}")
                return

            await self.order_new_status(update_order_status)

            if self.binance_order_id is None:
                self.binance_order_id = update_msg.get("exchange_order_id")

            if update_order_status == "NEW":
                self.is_placed = True
                order_placed_callbacks = self.callbacks.get("onOrderPlaced")

                for callback in order_placed_callbacks:
                    await self.create_task(callback[0], callback[1], self.order_id)

            filled_amount = update_msg.get("total_filled")
            if filled_amount is not None:
                filled_amount_float = float(filled_amount)
                if filled_amount_float > float(self.filled_amount):
                    self.filled_amount = filled_amount
                    self.commission = update_msg.get("commission")
                    self.commission_asset = update_msg.get("commission_asset")

            if update_order_status in final_order_status:
                self.is_placed = False
                self.order_executed_timestamp = update_msg.get("timestamp")
                self.api_handler.unsubscribe_from_order_update(self.order_id)

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    await self.create_task(callback[0], callback[1])

                final_callback = self.callbacks.get("readyToDie")

                if len(final_callback) == 1:
                    final_callback = final_callback[0]
                    await self.create_task(final_callback[0], final_callback[1], self.order_id,
                                           self.async_tasks)  # should not switch context b4 append happens
                else:
                    self.logger.error(
                        f"Final callback for order {self.order_id} should be always only 1 while it is {len(final_callback)}")

        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = super().dump_order_data()
            output["action_price"] = self.action_price
            return output
        except Exception as e:
            handle_exception(self.logger, e)


class StopMarketOrder(Order):
    def __init__(self, api_handler, logger, symbol: str, side: str, trigger_price,
                 base_amount=None, quote_amount=None, order_id: str = None, is_repay: bool = False, parent_order_id: str = None):
        super(StopMarketOrder, self).__init__(api_handler, logger, "STOP_LOSS", symbol, side,
                                              base_amount, quote_amount, order_id, is_repay, parent_order_id)

        self.average_price = 0
        self.trigger_price = trigger_price
        self.callbacks["onOrderPlaced"] = []
        self.order_data["trigger_price"] = self.trigger_price

    async def place_order(self):
        try:
            self.api_handler.subscribe_on_order_update(self.order_id, [StopMarketOrder.updates_handler, self])
            arguments = {
                "symbol": self.symbol,
                "side": self.side,
                "trigger_price": self.trigger_price,
                "quote_amount": self.quote_amount,
                "base_amount": self.base_amount,
                "order_id": self.order_id,
                "is_repay": self.is_repay
            }

            loop = asyncio.get_event_loop()
            upd_msg, is_filtered = await loop.run_in_executor(None,
                                                              functools.partial(self.api_handler.place_stop_order, **arguments))
            status = upd_msg.get("order_status") if isinstance(upd_msg, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": upd_msg}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Stop Market order {self.order_id} failed to be placed: {upd_msg['returned_status']}"
                self.logger.error(error_msg)
                is_filtered = False
            if not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)
            return upd_msg

        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self):
        try:
            response, is_filtered = self.api_handler.cancel_running_order(self.symbol, self.order_id)
            status = response.get("order_status") if isinstance(response, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Stop Market order {self.order_id} failed to be canceled: {response}"
                self.logger.error(error_msg)
                await self.handle_cancel_order_error(response, self.updates_handler)
            if not is_filtered:
                await self.create_task(self.updates_handler, response)
        except Exception as e:
            handle_exception(self.logger, e)

    async def updates_handler(self, update_msg):
        try:
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Stop market order {self.order_id} got update {update_msg}"))
            try:
                update_order_status = update_msg.get("order_status")
            except:
                update_order_status = None

            if update_order_status is None:
                self.logger.error(f"Received order update with no status: {update_msg}")
                return

            order_id = update_msg.get("client_order_id")

            if order_id != self.order_id:
                self.logger.error(f"Received update with order_id {order_id} while assigned was {self.order_id}")
                return

            await self.order_new_status(update_order_status)
            loop = asyncio.get_event_loop()
            update_order_type = update_msg.get("order_type")

            if self.binance_order_id is None:
                self.binance_order_id = update_msg.get("exchange_order_id")

            if update_order_status == "NEW":
                if update_order_type == "MARKET":
                    await self.order_new_status("Trigger Price was reached and Market order was placed")
                else:
                    self.is_placed = True
                    order_placed_callbacks = self.callbacks.get("onOrderPlaced")
                    for callback in order_placed_callbacks:
                        await self.create_task(callback[0], callback[1], self.order_id)

            if update_order_status in final_order_status:
                if update_order_status == "EXPIRED":
                    self.logger.debug(
                        f"{self.order_type} {self.order_id} order expired to place a market order")
                    return

                self.is_placed = False
                self.order_executed_timestamp = update_msg.get("timestamp", time.time())
                self.api_handler.unsubscribe_from_order_update(self.order_id)

                if update_order_status == "FILLED":
                    print(update_msg)
                    self.average_price = update_msg.get("avg_price")

                filled_amount = update_msg.get("total_filled")
                if filled_amount is not None:
                    filled_amount_float = float(filled_amount)
                    if filled_amount_float > float(self.filled_amount):
                        self.filled_amount = filled_amount
                        self.commission = update_msg.get("commission")
                        self.commission_asset = update_msg.get("commission_asset")

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    await self.create_task(callback[0], callback[1], self.order_id)

                final_callback = self.callbacks.get("readyToDie")

                if len(final_callback) == 1:
                    final_callback = final_callback[0]
                    task = asyncio.create_task(final_callback[0](final_callback[1], self.order_id,
                                                                 self.async_tasks))  # should not switch context b4 append happens
                    self.async_tasks.append(task)
                else:
                    self.logger.error(
                        f"Final callback for order {self.order_id} should be always only 1 while it is {len(final_callback)}")

        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = super().dump_order_data()
            output["avg_price"] = self.average_price
            output["trigger_price"] = self.trigger_price
            return output
        except Exception as e:
            handle_exception(self.logger, e)


class StopLimitOrder(Order):    
    def __init__(self, api_handler, logger, symbol: str, side: str, trigger_price, action_price,
                 base_amount=None, quote_amount=None, order_id: str = None, is_repay: bool = False, parent_order_id: str = None):
        super(StopLimitOrder, self).__init__(api_handler, logger, "STOP_LOSS_LIMIT", symbol, side,
                                             base_amount, quote_amount, order_id, is_repay, parent_order_id)

        self.trigger_price = trigger_price
        self.action_price = action_price

        self.callbacks["onOrderPlaced"] = []
        self.order_data["trigger_price"] = self.trigger_price
        self.order_data["action_price"] = self.action_price

    async def place_order(self):
        try:
            self.api_handler.subscribe_on_order_update(self.order_id, [StopLimitOrder.updates_handler, self])
            arguments = {
                "symbol": self.symbol,
                "side": self.side,
                "trigger_price": self.trigger_price,
                "action_price": self.action_price,
                "quote_amount": self.quote_amount,
                "base_amount": self.base_amount,
                "order_id": self.order_id,
                "is_repay": self.is_repay
            }

            loop = asyncio.get_event_loop()
            response, is_filtered = await loop.run_in_executor(None,
                                                  functools.partial(self.api_handler.place_stop_order, **arguments))
            status = response.get("order_status") if isinstance(response, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Stop limit order {self.order_id} failed to be placed: {response}"
                self.logger.error(error_msg)
                is_filtered = False
            if not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)
            return upd_msg

        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self):
        try:
            response, is_filtered = self.api_handler.cancel_running_order(self.symbol, self.order_id)
            status = response.get("order_status") if isinstance(response, dict) else None

            upd_msg = {"order_status": status, "client_order_id": self.order_id, "returned_status": response}
            if status is None:
                upd_msg["order_status"] = "FAILED"
                error_msg = f"Stop Limit order {self.order_id} failed to be canceled: {response}"
                self.logger.error(error_msg)
                await self.handle_cancel_order_error(response, self.updates_handler)

            if not is_filtered:
                await self.create_task(self.updates_handler, upd_msg)

        except Exception as e:
            handle_exception(self.logger, e)

    async def updates_handler(self, update_msg):
        try:
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"Stop Limit order {self.order_id} update: {update_msg}"))

            update_order_status = update_msg.get("order_status")
            if update_order_status is None:
                self.logger.error(f"Received order update with no status: {update_msg}")
                return

            order_id = update_msg.get("client_order_id")
            if order_id != self.order_id:
                self.logger.error(f"Received update with order_id {order_id} while assigned was {self.order_id}")
                return

            loop = asyncio.get_event_loop()
            await self.order_new_status(update_order_status)
            update_order_type = update_msg.get("order_type")
            if self.binance_order_id is None:
                self.binance_order_id = update_msg.get("exchange_order_id")

            if update_order_status == "NEW":
                if update_order_type == "LIMIT":
                    await self.order_new_status("Trigger Price was reached and LIMIT order was placed")
                else:
                    self.is_placed = True
                    order_placed_callbacks = self.callbacks.get("onOrderPlaced")
                    for callback in order_placed_callbacks:
                        await self.create_task(callback[0], callback[1], self.order_id)

            filled_amount = update_msg.get("total_filled")
            if filled_amount is not None:
                filled_amount_float = float(filled_amount)
                if filled_amount_float > float(self.filled_amount):
                    self.filled_amount = filled_amount
                    self.commission = update_msg.get("commission")
                    self.commission_asset = update_msg.get("commission_asset")

            if update_order_status in final_order_status:
                if update_order_status == "EXPIRED":
                    self.logger.debug(
                        f"{self.order_type} {self.order_id} order expired to place a limit order")
                    return

                self.is_placed = False
                self.order_executed_timestamp = update_msg.get("timestamp")
                self.api_handler.unsubscribe_from_order_update(self.order_id)

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    await self.create_task(callback[0], callback[1], self.order_id)

                final_callback = self.callbacks.get("readyToDie")
                if len(final_callback) == 1:
                    final_callback = final_callback[0]
                    await self.create_task(final_callback[0], final_callback[1], self.order_id,
                                           self.async_tasks)  # should not switch context b4 append happens
                else:
                    self.logger.error(
                        f"Final callback for order {self.order_id} should be always only 1 while it is {len(final_callback)}")

        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = super().dump_order_data()
            output["trigger_price"] = self.trigger_price
            output["action_price"] = self.action_price

            return output
        except Exception as e:
            handle_exception(self.logger, e)
