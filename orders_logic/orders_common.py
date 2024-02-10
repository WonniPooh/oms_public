from common import *

class StopParams:
    def __init__(self, **kwargs):

        self.trigger_price = kwargs.get("trigger_price", None)
        self.stop_price = kwargs.get("stop_price", None)

        self.is_trailing = kwargs.get("is_trailing", False)
        self.is_avg_trailing = kwargs.get("is_avg_trailing", False)

        self.price_step = kwargs.get("price_step", None)

        self.was_triggered = False


base_orders_status = ["INITIALIZED", "NEW", "PARTIALLY FILLED", "FILLED", "CANCELED"]
final_order_status = ["FILLED", "CANCELED", "EXPIRED", "FAILED"]
