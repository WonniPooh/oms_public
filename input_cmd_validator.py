from common import *


class InputValidator():
    def __init__(self, logger):
        self.logger = logger

    def validate_new_cmd(self, command):
        try:
            self.logger.info(compose_log_msg(getframeinfo(currentframe()),
                                             f"New Cmd arrived: {command}"))

            cmd_type = command.get("type")

            if cmd_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Cmd without type arrived: {command}"))
                return False

            if cmd_type == "SWITCH PRICE SOURCE":
                return True

            if cmd_type == "PRICE ADJUST":
                order_id = command.get("order_id")
                if order_id is None:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Price adjust: order id is None: {command}"))
                    return False

                account_type = command.get("account_type")
                if account_type is None:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Price adjust: account type is None: {command}"))
                    return False

                price = command.get("price")
                if price is None:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"Price adjust: new price was not provided {command}"))
                    return False

                return True

            if cmd_type == "NEW ORDER":
                return self.validate_new_order_cmd(command)

            if cmd_type == "CANCEL":
                return self.validate_cancel_order_cmd(command)

            if cmd_type == "GET_INFO":
                section = command.get("section")
                if section is None:
                    return False
                if section == "BALANCE":
                    asset = command.get("asset")
                    if asset is None:
                        return False
                return True

        except Exception as e:
            handle_exception(self.logger, e)

    def validate_combined_order_cmd(self, command):
        try:
            required_order_params = ["close_order_params"]
            order_type = "COMBINED"

            check_result = self.check_required_params_present(required_order_params, command, order_type)
            if not check_result:
                return False

            open_order_params = command.get("open_order_params", {})
            open_order_type = open_order_params.get("order_type")

            if open_order_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd with open params but without open_order type: {command}"))
                return False

            close_order_params = command.get("close_order_params")

            if close_order_params is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without close order params: {command}"))
                return False

            close_order_type = close_order_params.get("order_type")
            if close_order_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without close order type: {command}"))
                return False

            input_type = close_order_params.get("input_type")
            if input_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without close order input_type param: {command}"))
                return False

            if close_order_type == "TRAILING_STOP":
                if close_order_params.get("stop_conditions") is None:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"New {order_type} stop TRAILING order cmd without close order stop_conditions param: {command}"))
                    return False

            if close_order_type == "INTELLECTUAL":
                params = ["stop_conditions", "take_conditions"]
                for param in params:
                    if close_order_params.get(param) is None:
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"New {order_type} order cmd CLOSE INTELLECTUAL without close order {param} param: {command}"))
                        return False

                    if not isinstance(close_order_params.get(param), dict):
                        self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                          f"New {order_type} order cmd CLOSE INTELLECTUAL {param} param should be a dict: {command}"))
                        return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_trailing_stop_order_cmd(self, command):
        try:
            order_type = "TRAILING_STOP"
            cmd_side = command.get("side").upper()
            required_order_params = ["stop_price_delta", "position_entrance_price", "take_price",
                                     "stop_price_delta_after_take", "price_min_step"]
            check_result = self.check_required_params_present(required_order_params, command, order_type)
            if not check_result:
                return False

            stop_price_delta = command.get("stop_price_delta")
            position_entrance_price = command.get("position_entrance_price")
            take_price = command.get("take_price")
            stop_price_delta_after_take = command.get("stop_price_delta_after_take")

            if position_entrance_price >= take_price and cmd_side == "SELL":
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} {cmd_side} order cmd with stop position_entrance_price price >= take_price price arrived: {command}"))
                return False

            if position_entrance_price <= take_price and cmd_side == "BUY":
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} {cmd_side} order cmd with stop position_entrance_price price <= take_price price arrived: {command}"))
                return False

            if stop_price_delta < stop_price_delta_after_take:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} {cmd_side} order cmd with stop_price_delta > stop_price_delta_after_take  arrived: {command}"))
                return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_oco_order_cmd(self, command):
        try:
            order_type = "OCO"

            cmd_price = command.get("limit_price")
            if cmd_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without price arrived: {command}"))
                return False

            cmd_price = command.get("trigger_price")
            if cmd_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without price arrived: {command}"))
                return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_stop_market_order_cmd(self, command):
        try:
            order_type = "STOP_MARKET"
            cmd_price = command.get("trigger_price")
            if cmd_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without price arrived: {command}"))
                return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_stop_limit_order_cmd(self, command):
        try:
            order_type = "STOP_LIMIT"
            trigger_price = command.get("trigger_price")
            action_price = command.get("action_price")

            if trigger_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without trigger price arrived: {command}"))
                return False

            if action_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without action price arrived: {command}"))
                return False

            if command.get("side").upper() == "SELL":
                if action_price > trigger_price:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"New {order_type} side SELL order has action price > trigger_price arrived: {command}"))
                    return False
            else:
                if action_price < trigger_price:
                    self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                      f"New {order_type} side BUY order has action price < trigger_price arrived: {command}"))
                    return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_limit_order_cmd(self, command):
        try:
            order_type = "LIMIT"
            cmd_price = command.get("action_price")

            if cmd_price is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New {order_type} order cmd without price arrived: {command}"))
                return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def validate_new_order_cmd(self, command):
        try:
            order_type = command.get("order_type")

            if order_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without order type arrived: {command}"))
                return False

            symbol = command.get("symbol")
            if symbol is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without symbol arrived: {command}"))
                return False

            cmd_side = command.get("side")
            if cmd_side is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without order side arrived: {command}"))
                return False

            cmd_account = command.get("account_type")
            if cmd_account is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without account specified arrived: {command}"))
                return False

            cmd_base = command.get("base")
            cmd_quote = command.get("quote")
            if cmd_base is None and cmd_quote is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without neither quote nor base asset amount arrived: {command}"))
                return False

            is_repay = command.get("is_repay")
            if cmd_account != "SPOT" and is_repay is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"New order cmd without is_repay arrived: {command}"))
                return False


            return True

        except Exception as e:
            handle_exception(self.logger, e)

    def validate_cancel_order_cmd(self, command):
        try:
            order_id = command.get("order_id")
            if order_id is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Cancel order cmd: without order id specified: {command}"))
                return False

            account_type = command.get("account_type")
            if account_type is None:
                self.logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                  f"Cancel order cmd: without account_type specified: {command}"))
                return False

            return True
        except Exception as e:
            handle_exception(self.logger, e)

    def check_required_params_present(self, required_params, command, order_type):
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
