from .orders_common import *
from .basic_orders import Order, StopLimitOrder, StopMarketOrder, MarketOrder

class TrailingStopOrder(Order):
    def __init__(self, api_handler, logger, symbol: str, side: str,
                 position_entrance_price: float, stop_data: list,
                 base_amount: float = None, order_id: str = None, is_repay: bool = False,
                 is_stop_limit: bool = False, parent_order_id: str = None):
        super(TrailingStopOrder, self).__init__(api_handler, logger, "TRAILING_STOP", symbol, side,
                                                base_amount, None, order_id, is_repay, parent_order_id)
        try:
            self.callbacks["onOrderPlaced"] = []
            self.order_canceled = False
            self.stop_order = None
            self.is_stop_limit = is_stop_limit
            self.should_process_initial_stop = False

            self.enter_price = position_entrance_price
            self.default_price_min_step = position_entrance_price * 0.002

            self.current_price_min_step = self.default_price_min_step

            sort_getter = lambda x: x.trigger_price if x.trigger_price is not None else -1
            if side == "SELL":
                self.stop_conditions = sorted(stop_data, key=sort_getter)
            else:
                self.stop_conditions = sorted(stop_data, key=sort_getter, reverse=True)
                if self.stop_conditions[-1].trigger_price is None:
                    stop = self.stop_conditions.pop(-1)
                    self.stop_conditions.insert(0, stop)

            self.current_stop_price_delta = None

            if self.stop_conditions[0].trigger_price is None:  # trigger is None = initial stop
                self.current_stop_price_delta = self.enter_price - self.stop_conditions[0].stop_price
                if self.stop_conditions[0].price_step is not None:
                    self.current_price_min_step = self.stop_conditions[0].price_step
                self.stop_conditions[0].was_triggered = True

            if self.current_stop_price_delta:
                if self.side == "SELL":
                    self.current_stop_price = self.enter_price - self.current_stop_price_delta
                else:
                    self.current_stop_price = self.enter_price + self.current_stop_price_delta

                self.construct_child_order()
                self.should_process_initial_stop = True

            self.current_extremal_price_reached = float(self.enter_price)
            self.price_update_callback_id = None

            self.liquidation_order = None

            self.order_is_placed = False
            self.update_needed = False
            self.liquidation_fired = False

            for condition in self.stop_conditions:
                stop_cond = {
                    "is_trailing": True,
                    "is_avg_trailing": False,
                    "is_limit_order": self.is_stop_limit,
                    "trigger_price": condition.trigger_price if condition.trigger_price is not None else "NULL",
                    "stop_price": condition.stop_price,
                    "enter_position_price": self.enter_price,
                    "trailing_price_step": self.default_price_min_step if condition.price_step is None else condition.price_step
                }

        except Exception as e:
            handle_exception(self.logger, e)

    def construct_child_order(self):
        try:
            m_args = {
                "api_handler": self.api_handler,
                "logger": self.logger,
                "symbol": self.symbol,
                "side": self.side,
                "trigger_price": self.current_stop_price,
                "base_amount": self.base_amount,
                "is_repay": self.is_repay
            }
            if self.is_stop_limit:
                m_args["action_price"] = self.current_stop_price
                self.stop_order = StopLimitOrder(**m_args)
            else:
                self.stop_order = StopMarketOrder(**m_args)
            self.stop_order.callbacks["onOrderPlaced"].append([TrailingStopOrder.on_child_order_placed, self])
            self.stop_order.callbacks["readyToDie"].append([TrailingStopOrder.on_child_order_finalized, self])
        except Exception as e:
            handle_exception(self.logger, str(e))

    async def price_update_handler(self, last_price):
        try:
            last_price = float(last_price)

            if self.should_process_initial_stop:
                self.should_process_initial_stop = False
                await self.place_child_order()
                return

            if self.side == "SELL":

                for adjust in self.stop_conditions:
                    was_already_fired = adjust.was_triggered
                    if was_already_fired:
                        continue

                    trigger_price = adjust.trigger_price
                    if last_price >= trigger_price:
                        adjust.was_triggered = True
                        self.current_stop_price_delta = abs(adjust.trigger_price - adjust.stop_price)
                        self.current_price_min_step = adjust.price_step if adjust.price_step is not None else self.default_price_min_step
                        self.current_stop_price = self.current_extremal_price_reached - self.current_stop_price_delta
                        self.update_needed = True
                        self.logger.debug(
                            f"Stop adjust fired at price {trigger_price}; Current_stop:{self.current_stop_price}")

                if last_price >= self.current_extremal_price_reached + self.current_price_min_step:
                    self.current_extremal_price_reached = last_price
                    self.current_stop_price = self.current_extremal_price_reached - self.current_stop_price_delta
                    self.update_needed = True
                    self.logger.debug(f"Adjust stop: new price is {self.current_stop_price}")

            else:
                for adjust in self.stop_conditions:
                    was_already_fired = adjust.was_triggered
                    if was_already_fired:
                        continue

                    trigger_price = adjust.trigger_price
                    if last_price <= trigger_price:
                        adjust.was_triggered = True
                        self.current_stop_price_delta = abs(adjust.trigger_price - adjust.stop_price)
                        self.current_price_min_step = adjust.price_step if adjust.price_step is not None else self.default_price_min_step
                        self.current_stop_price = self.current_extremal_price_reached + self.current_stop_price_delta
                        self.update_needed = True
                        self.logger.debug(
                            f"Stop adjust fired at price {trigger_price}; Current_stop:{self.current_stop_price}")

                if last_price <= self.current_extremal_price_reached - self.current_price_min_step:
                    self.current_extremal_price_reached = last_price
                    self.current_stop_price = self.current_extremal_price_reached + self.current_stop_price_delta
                    self.update_needed = True
                    self.logger.debug(f"Adjust stop: new price is {self.current_stop_price}")

            if self.order_is_placed and self.update_needed:
                self.order_is_placed = False
                self.update_needed = False
                if self.is_stop_limit:
                    self.stop_order.action_price = self.current_stop_price
                self.stop_order.trigger_price = self.current_stop_price
                await self.create_task(self.stop_order.cancel_order)

            if self.stop_order is None and self.current_stop_price_delta is not None:
                self.update_needed = False
                self.construct_child_order()
                await self.place_child_order()



        except Exception as e:
            handle_exception(self.logger, e)

    async def place_child_order(self):
        try:
            if self.stop_order.current_order_status != "INITIALIZED":
                await self.stop_order.order_new_status("INITIALIZED")
            await self.create_task(self.stop_order.place_order)
        except Exception as e:
            handle_exception(self.logger, e)

    async def on_child_order_placed(self, order_id):
        try:
            if order_id != self.stop_order.order_id:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Got update from order {order_id} while waited for update ONLy from {self.stop_order.order_id}"))
                return
            self.order_is_placed = True
        except Exception as e:
            handle_exception(self.logger, e)

    async def on_child_order_finalized(self, order_id, tasks):
        try:
            finalized_order = self.stop_order if self.stop_order.order_id == order_id else self.liquidation_order
            self.async_tasks += tasks
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"{self.order_id} Trailing child order {finalized_order.order_type}:{order_id} finalized with status {finalized_order.current_order_status}"))

            if finalized_order.current_order_status == "CANCELED" and not self.order_canceled:
                await self.place_child_order()
            else:
                self.is_placed = False
                if self.price_update_callback_id:
                    self.api_handler.unsubscribe_from_price_update(self.symbol, self.price_update_callback_id)
                    self.price_update_callback_id = None

                if self.liquidation_fired and finalized_order.order_type != "MARKET":
                    return

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    await self.create_task(callback[0], callback[1])

                final_callback = self.callbacks.get("readyToDie")
                if len(final_callback) == 1:
                    final_callback = final_callback[0]
                    await self.create_task(final_callback[0], final_callback[1], self.order_id, self.async_tasks)
                else:
                    self.logger.error(
                        f"Final callback for order {self.order_id} should be always only 1 while it is {len(final_callback)}")

            filled_amount = finalized_order.filled_amount
            if filled_amount is not None:
                self.filled_amount = filled_amount
        except Exception as e:
            handle_exception(self.logger, e)

    async def place_order(self):
        try:
            await self.order_new_status("Place CMD received")
            self.is_placed = True
            self.price_update_callback_id = self.api_handler.subscribe_for_price_update(self.symbol, [TrailingStopOrder.price_update_handler, self])
        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self, should_liquidate=False):
        try:
            await self.order_new_status("Cancel CMD was received")
            self.order_canceled = True
            if self.price_update_callback_id:
                self.api_handler.unsubscribe_from_price_update(self.symbol, self.price_update_callback_id)
                self.price_update_callback_id = None
            if self.stop_order.is_placed:
                self.order_is_placed = False
                await self.stop_order.cancel_order()

            if should_liquidate:
                self.liquidation_fired = True
                await self.liquidate_position()
        except Exception as e:
            handle_exception(self.logger, e)

    async def liquidate_position(self):
        try:
            amount_to_liquidate = float(self.stop_order.base_amount) - float(self.stop_order.filled_amount)
            min_order_amount = float(self.api_handler.get_min_base_order_amount(self.symbol))

            if min_order_amount > amount_to_liquidate:
                self.logger.error("Can't liquidate: Amount left is less than min order amount")
                return

            order_args = {
                "api_handler": self.api_handler,
                "logger": self.logger,
                "symbol": self.symbol,
                "side": self.side,
                "quote_amount": None,
                "base_amount": amount_to_liquidate,
                "is_repay": True
            }
            self.logger.debug(compose_log_msg(getframeinfo(currentframe()),
                                              f"{self.order_id} Trailing child order is liquidating; amount is {amount_to_liquidate} {self.symbol}"))
            self.liquidation_order = MarketOrder(**order_args)
            self.liquidation_order.callbacks["readyToDie"].append([TrailingStopOrder.on_child_order_finalized, self])
            await self.liquidation_order.place_order()

        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = super().dump_order_data()

            stop_output = []
            for cond in self.stop_conditions:
                stop_output.append(vars(cond))

            output.insert(7, ("enter_price", self.enter_price))
            output.insert(8, ("stop_conditions", stop_output))
            child_dump = self.stop_order.dump_order_data()
            output.insert(9, ("child_order_dump", child_dump))

            return dict(output)
        except Exception as e:
            handle_exception(self.logger, e)
