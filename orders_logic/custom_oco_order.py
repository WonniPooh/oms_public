from .orders_common import *
from .basic_orders import Order, LimitOrder, StopLimitOrder, StopMarketOrder

#TODO totally unhandled part of partially filled
class CustomOcoOrder(Order):
    def __init__(self, api_handler, logger,
                 symbol: str, side: str,
                 stop_type: str, limit_price: float,
                 trigger_price: float, action_price: float = None,
                 base_amount: float = None, quote_amount: float = None,
                 order_id: str = None, is_repay: bool = False):
        super(CustomOcoOrder, self).__init__(api_handler, logger, "CUSTOM_OCO", symbol, side,
                                             base_amount, quote_amount, order_id, is_repay)

        self.is_repay = is_repay
        self.stop_type = stop_type
        self.limit_order_price = float(limit_price)
        self.stop_order_trigger = float(trigger_price)
        if action_price is not None:
            self.stop_order_action = float(action_price)

        self.callbacks["onOrderPlaced"] = []
        self.price_update_callback_id = None

        price_delta = abs(self.limit_order_price - self.stop_order_trigger)
        self.limit_switchover = self.limit_order_price - 0.35*price_delta
        self.slt_switchover = self.stop_order_trigger + 0.35*price_delta
        if self.limit_order_price < self.stop_order_trigger:
            self.limit_switchover = self.limit_order_price + 0.35 * price_delta
            self.slt_switchover = self.stop_order_trigger - 0.35 * price_delta

        self.current_order_to_run = "stop_limit"
        self.order_canceled = False

        lt_args = {
            "api_handler": self.api_handler,
            "logger": self.logger,
            "symbol": self.symbol,
            "side": self.side,
            "quote_amount": self.quote_amount,
            "base_amount": self.base_amount,
            "is_repay": self.is_repay
        }

        slt_args = lt_args.copy()
        slt_args["trigger_price"] = self.stop_order_trigger
        if self.stop_type == "STOP_LIMIT":
            slt_args["action_price"] = self.stop_order_trigger if self.stop_order_action is None else self.stop_order_action
            self.stop_order = StopLimitOrder(**slt_args)
        self.stop_order = StopMarketOrder(**slt_args)
        self.stop_order.callbacks["readyToDie"].append([CustomOcoOrder.on_child_order_finalized, self])

        lt_args["action_price"] = self.limit_order_price
        self.limit_order = LimitOrder(**lt_args)
        self.limit_order.callbacks["readyToDie"].append([CustomOcoOrder.on_child_order_finalized, self])

    async def price_update_handler(self, last_price):
        try:
            print("Real price handler:", last_price, self.limit_switchover,
                  self.slt_switchover, self.limit_order.is_placed,
                  self.stop_order.is_placed, self.current_order_to_run)
            if self.side == "SELL":
                if last_price >= self.limit_switchover and not self.limit_order.is_placed:
                    print("switch to limit")
                    if self.stop_order.is_placed:
                        self.current_order_to_run = "limit"
                        await self.create_task(self.stop_order.cancel_order)

                if last_price <= self.slt_switchover and not self.stop_order.is_placed:
                    print("switch to stop limit")
                    if self.limit_order.is_placed:
                        self.current_order_to_run = "stop_limit"
                        await self.create_task(self.limit_order.cancel_order)
            else:
                if last_price <= self.limit_switchover and not self.limit_order.is_placed:
                    print("switch to limit")
                    if self.stop_order.is_placed:
                        self.current_order_to_run = "limit"
                        await self.create_task(self.stop_order.cancel_order)

                if last_price >= self.slt_switchover and not self.stop_order.is_placed:
                    print("switch to stop limit")
                    if self.limit_order.is_placed:
                        self.current_order_to_run = "stop_limit"
                        await self.create_task(self.limit_order.cancel_order)

        except Exception as e:
            handle_exception(self.logger, e)

    async def on_child_order_finalized(self, order_id, tasks):
        try:
            finalized_order = self.limit_order if order_id == self.limit_order.order_id else self.stop_order
            print(f"Oco child order {finalized_order.order_type}:{order_id} finalized with status {finalized_order.current_order_status}")
            self.async_tasks += tasks
            if finalized_order.current_order_status == "CANCELED" and not self.order_canceled:
                await self.place_child_order()
            else:
                self.is_placed = False

                order_finalized_callbacks = self.callbacks.get("onOrderFinalized")
                for callback in order_finalized_callbacks:
                    print("On order finalized:", callback)
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

    async def place_child_order(self):
        try:
            order_to_activate = self.limit_order if self.current_order_to_run == "limit" else self.stop_order
            print(f"Activating {order_to_activate.order_type}")
            await order_to_activate.order_new_status("INITIALIZED")
            await self.create_task(order_to_activate.place_order)
        except Exception as e:
            handle_exception(self.logger, e)

    async def place_order(self):
        try:
            await self.create_task(self.stop_order.place_order)
            await asyncio.sleep(5)
            self.price_update_callback_id = self.api_handler.subscribe_for_price_update(self.symbol,
                                                                                        [CustomOcoOrder.price_update_handler, self])
        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self):
        try:
            self.order_canceled = True
            self.api_handler.unsubscribe_from_price_update(self.symbol, self.price_update_callback_id)
            if self.limit_order.is_placed:
                await self.limit_order.cancel_order()
            elif self.stop_order.is_placed:
                await self.stop_order.cancel_order()
        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        pass

