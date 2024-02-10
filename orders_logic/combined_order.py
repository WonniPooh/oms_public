from .orders_common import *
from .basic_orders import Order, MarketOrder, LimitOrder
from .trailing_stop_order import TrailingStopOrder, StopParams


class CombinedOrder(Order):
    """
    side - open position order side, close position order side wil be opposite
    quote_amount - for "open position" order, "close position" order amount will be automatically calculated based on "open" result
    base_amount  - for "open position" order, "close position" order amount will be automatically calculated based on "open" result
    is_repay - is "close position" order repay
    close order params:
        input_type:PERC|ACTUAL
        oreder_type:TRAILING|INTELLECTUAL
        if TRAILING:
            stop_conditions - stop price | in percent
            take_data - take price | in percent

            stop_conditions_after_take
            price_step

        if INTELLECTUAL:
            is_trailing - should use trailing avg stop
            stop_conditions - dict: {"initial": stop_price val or % if input is PERC;
                               "adjust_trailing": same as initial, can be absent}
            take_data - dict of action_price|change_perc : perc of filled amount in open_position
    """

    def __init__(self, api_handler, logger, symbol: str, side: str,
                 close_order_params: dict, open_order_params: dict = None,
                 base_amount: float = None, quote_amount: float = None,
                 order_id: str = None, is_repay: bool = True, parent_order_id: str = None):
        super(CombinedOrder, self).__init__(api_handler, logger, "COMBINED", symbol, side,
                                            base_amount, quote_amount, order_id, is_repay, parent_order_id)

        self.order_canceled = False
        self.callbacks["onOrderPlaced"] = []
        self.default_price_step_perc = 0.1

        self.is_close_a_repay = is_repay
        self.close_order_params = close_order_params
        self.close_position_order = None
        self.close_order_was_placed = False

        self.open_order_params = open_order_params

        self.order_data["open_order_type"] = self.open_order_params["order_type"]
        self.order_data["close_order_type"] = self.close_order_params["order_type"]
        self.order_data["close_order_params"] = self.close_order_params

        open_args = {
            "api_handler": self.api_handler,
            "logger": self.logger,
            "symbol": self.symbol,
            "side": self.side,
            "quote_amount": self.quote_amount,
            "base_amount": self.base_amount,
        }

        estimated_amount = 0
        price = self.open_order_params.get("action_price") if self.open_order_params is not None else None
        if self.base_amount is not None:
            estimated_amount = float(self.api_handler.apply_amount_precision(self.symbol, open_args["base_amount"], price))
        else:
            estimated_amount = float(self.api_handler.calculate_quantity(self.symbol, open_args["quote_amount"], price))

        if estimated_amount == 0:
            self.logger.info(f"Order {self.order_id} can't be placed: estimated possible quote amount to buy is 0")
            self.order_new_status_sync("FAILED")
            self.finalize()
            return

        open_order_type = self.open_order_params["order_type"]
        if open_order_type == "LIMIT" and self.open_order_params is not None:
            open_args["action_price"] = self.open_order_params.get("action_price")

        self.open_position_order = None
        if open_order_type == "MARKET":
            self.open_position_order = MarketOrder(**open_args)
        elif open_order_type == "LIMIT":
            self.open_position_order = LimitOrder(**open_args)
        else:
            self.logger.critical(compose_log_msg(getframeinfo(currentframe()),
                                        f"New COMBINED order open position is neither LIMIT nor MARKET:{open_order_type}"))
            return

        self.open_position_order.callbacks["readyToDie"].append([CombinedOrder.on_child_order_finalized, self])

    async def price_update_handler(self, last_price):
        try:
            await self.create_task(self.close_position_order.price_update_handler, last_price)
        except Exception as e:
            handle_exception(self.logger, e)

    async def on_child_order_finalized(self, order_id, tasks):
        try:
            finalized_order = self.open_position_order if self.close_order_was_placed is False else self.close_position_order
            self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"Combined child order {finalized_order.order_type}:{order_id} finalized with status {finalized_order.current_order_status}"))
            self.async_tasks += tasks

            if finalized_order.current_order_status == "FILLED" and self.close_order_was_placed is False:
                await self.order_new_status("Finalized Open order; Constructing Close position order")
                await self.construct_close_order()
                await self.close_position_order.place_order()
                self.close_order_was_placed = True
                await self.order_new_status("Placed close position order")
                return

            if finalized_order.current_order_status == "CANCELED":
                if not self.order_canceled:
                    self.logger.critical(compose_log_msg(getframeinfo(currentframe()),
                                                         f"Child order {finalized_order.order_type} got canceled while parent order does not receive order to cancel"))

            await self.finalize()
            filled_amount = finalized_order.filled_amount
            if filled_amount is not None:
                self.filled_amount = filled_amount

        except Exception as e:
            handle_exception(self.logger, e)

    async def construct_close_order(self):
        try:
            base_amount = self.open_position_order.filled_amount
            if self.open_position_order.commission_asset == self.open_position_order.symbol[:len(self.open_position_order.commission_asset)]:
                base_amount = str(
                    round(float(base_amount) - float(self.open_position_order.commission), ROUND_PRECISION))

            close_args = {
                "api_handler": self.api_handler,
                "logger": self.logger,
                "symbol": self.symbol,
                "side": "BUY" if self.side == "SELL" else "SELL",
                "base_amount": base_amount,
                "is_repay": self.is_close_a_repay,
                "parent_order_id": self.order_id
            }

            open_price = self.open_position_order.action_price if self.open_position_order.order_type == "LIMIT" else self.open_position_order.average_price
            open_price = float(open_price)
            default_price_step = 0.002 * open_price
            close_order_type = self.close_order_params["order_type"]

            if close_order_type == "TRAILING_STOP":
                #Input for PERC should be 5 - 5% higher from enter; -5 - 5% lower then enter  price_step - 3 - 3% - only positive;
                #Input for actual price - actual prices + step absolute value
                #Input as dict: {trigger; stop; price_step}

                processed_stop_data = []
                if self.close_order_params["input_type"] == "PERC":
                    stop_data = self.close_order_params["stop_conditions"]

                    for condition in stop_data:
                        trigger = open_price + float(condition["trigger"]) / 100 * open_price if condition.get("trigger") is not None else None
                        stop_price = open_price + float(condition["stop"]) / 100 * open_price
                        price_step = abs(float(self.close_order_params["price_step"])) / 100 * open_price if self.close_order_params.get("price_step") else default_price_step
                        processed_stop_data.append(StopParams(**{"trigger_price": trigger,
                                                                 "stop_price": stop_price,
                                                                 "price_step": price_step,
                                                                 "is_trailing": True}))

                    close_args["position_entrance_price"] = open_price
                    close_args["stop_conditions"] = processed_stop_data

                else:
                    close_args["position_entrance_price"] = open_price
                    stop_data = self.close_order_params["stop_conditions"]

                    for condition in stop_data:
                        processed_stop_data.append(StopParams(**{"trigger_price": float(condition.get("trigger")) if condition.get("trigger") else None,
                                                                 "stop_price": float(condition.get("stop")),
                                                                 "price_step": float(condition.get("price_step")),
                                                                 "is_trailing": True}))
                    close_args["stop_conditions"] = processed_stop_data

                self.close_position_order = TrailingStopOrder(**close_args)

            self.close_position_order.callbacks["readyToDie"].append([CombinedOrder.on_child_order_finalized, self])

        except Exception as e:
            handle_exception(self.logger, e)

    async def place_order(self):
        try:
            self.is_placed = True
            await self.create_task(self.open_position_order.place_order)
            await self.order_new_status("Place CMD received")
        except Exception as e:
            handle_exception(self.logger, e)

    async def cancel_order(self, should_liquidate=False):
        try:
            self.logger.info(
                f"Cancel {self.order_type} {self.order_id} order with liquidate = {should_liquidate} was called")

            self.order_canceled = True
            if self.open_position_order and self.open_position_order.is_placed:
                await self.open_position_order.cancel_order()
            elif self.close_position_order and self.close_position_order.is_placed:
                await self.close_position_order.cancel_order(should_liquidate=should_liquidate)
        except Exception as e:
            handle_exception(self.logger, e)

    async def finalize(self):
        try:
            self.is_placed = False
            await self.order_new_status("Finalizing")
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
        except Exception as e:
            handle_exception(self.logger, e)

    def dump_order_data(self):
        try:
            output = super().dump_order_data()
            open_order_dump = {}
            close_order_dump = {}

            if self.open_position_order is not None:
                open_order_dump = self.open_position_order.dump_order_data()
            if self.close_position_order is not None:
                close_order_dump = self.close_position_order.dump_order_data()

            output["open_order_dump"] = open_order_dump
            output["close_order_dump"] = close_order_dump

            return output
        except Exception as e:
            handle_exception(self.logger, e)
