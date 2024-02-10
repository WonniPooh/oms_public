#TODO - quote recalculation can cause lack of asset for TP orders -  Combined order
#TODO - maybe should make 2 types of account balance - actual and within ordr manager one, which will consider amounts to be used in orders
import asyncio
import time

from common import *
import threading
from account_manager import accountManager
from input_websocket_server import *

managers = {}
tcp_connections_count = 0

async def shut_system():
    try:
        for manager_id in list(managers.keys()):
            await managers[manager_id].finalize_service()  # ?should a task be created for this?
            del managers[manager_id]

        tasks = asyncio.all_tasks()

        for _task in tasks:
            print('cancelling task')
            _task.cancel()

        loop = asyncio.get_event_loop()
        loop.stop()
        loop.close()

        return
    except Exception as e:
        print(e)

async def update_managed_accounts():
    with open("users.json", "r") as f:
        users = f.read()

    users = json.loads(users)

    global managers
    managers_to_drop = []
    for user_data in users:
        username = user_data["user"]

        if managers.get(username) is None:
            public = user_data["public"]
            secret = user_data["private"]
            manager = await accountManager.create(username=username, public=public, private=secret)
            managers[username] = manager

        # if managers.get(username) is not None:
        #     managers_to_drop.append(username)

    for username in managers_to_drop:
        await managers[username].finalize_service() #?should a task be created for this?
        del managers[username]

async def main():

    await update_managed_accounts()
    websocket_handlers["shut_down"] = shut_system
    websocket_handlers["update_accounts"] = update_managed_accounts
    websocket_handlers["managers"] = managers

    x = threading.Thread(target=run_socket_server)
    x.start()


if __name__ == "__main__":
    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    main_loop.run_until_complete(main())
    main_loop.run_forever()


# {"type":"PRICE ADJUST", "price":1.4 }

# {"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"MARKET", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 1, "base":10 }
# {"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"MARKET", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 1, "base":20 }

# {"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"LIMIT", "symbol":"ADAUSDT", "side":"SELL", "is_repay": 0, "price":1.555, "base":10 }
# {"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"LIMIT", "symbol":"ADAUSDT", "side":"SELL", "is_repay": 0, "price":1.555, "quote":33 }

#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"STOP_LIMIT", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "trigger_price":1.555, "action_price":1.625, "base":10 }

#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"TRAILING_AVG", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "position_entrance_price":1.40, "stop_price":1.50, "price_min_step":0.01, "base":20 }
#{"type":"PRICE ADJUST", "price":1.38 } - 1.59
#{"type":"PRICE ADJUST", "price":1.40 } - 1.55






# {"type":"NEW ORDER", "order_type":"MARKET", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "quote":30 }
# {"type":"NEW ORDER", "order_type":"LIMIT", "symbol":"ADAUSDT", "side":"SELL", "is_repay": 0, "price":1.555, "base":10 }
# {"type":"NEW ORDER", "order_type":"LIMIT", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "price":1.266536, "base":10 }

# {"type":"NEW ORDER", "order_type":"STOP_MARKET", "symbol":"ADAUSDT", "side":"SELL", "is_repay": 0, "trigger_price":1.555, "base":10 }
# {"type":"NEW ORDER", "order_type":"STOP_MARKET", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "trigger_price":1.766536, "base":10 }

# {"type":"NEW ORDER", "order_type":"STOP_LIMIT", "symbol":"ADAUSDT", "side":"SELL", "is_repay": 0, "trigger_price":1.555, "action_price":1.455, "base":10 }
# {"type":"NEW ORDER", "order_type":"STOP_LIMIT", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "trigger_price":1.766536, "action_price":1.80, "base":10 }


# {"type":"NEW ORDER", "order_type":"CUSTOM_OCO", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "lt_price":1.285, "slt_price":1.55, "base":50 }




#{"type":"NEW ORDER", "order_type":"TRAILING", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "position_entrance_price":1.55, "take_price":1.45, "stop_price_delta":0.2, "stop_price_delta_after_take":0.1, "price_min_step":0.01, "base":20 }
#{"type":"PRICE ADJUST", "price":1.545 }
#{"type":"PRICE ADJUST", "price":1.536 }
#{"type":"PRICE ADJUST", "price":1.535 }
#{"type":"PRICE ADJUST", "price":1.42 }



#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"MARKET", "symbol":"ADAUSDT", "side":"BUY", "is_repay": 0, "quote":30 }

#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"COMBINED", "symbol":"ADAUSDT", "side":"BUY", "quote":33, "close_order_type":"TRAILING", "close_order_params":{"input_type":"PERC", "stop_data":10, "take_data":5, "stop_data_after_take":7, "price_step":0.1}, "is_repay": 1}

#{"type":"NEW ORDER", "account_type":"MARGIN", "order_type":"COMBINED", "symbol":"ADAUSDT", "side":"BUY", "quote":50, "close_order_type":"INTELLECTUAL", "close_order_params":{"input_type":"ACTUAL", "stop_data":1.15, "take_data":{"1.34":40, "1.40":30, "1.45":30}}, "is_repay": 1}

                                                                                                                                                                                                                                                #    [trigger, {is_trailing, new_stop, new_step}]
#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"COMBINED", "symbol":"ADAUSDT", "side":"BUY", "quote":50, "close_order_type":"INTELLECTUAL", "close_order_params":{"input_type":"ACTUAL", "is_avg_trailing":1, "stop_data":{"initial":1.3, "stop_adjust":{""} }, "take_data":{"1.45":40, "1.50":30, "1.55":30}}, "is_repay": 1}


#{"type":"NEW ORDER", "account_type":"FUTURES", "order_type":"COMBINED", "symbol":"ADAUSDT", "side":"BUY", "quote":50, "close_order_type":"TRAILING", "close_order_params":{"input_type":"ACTUAL", "stop_data":[{"trigger":0.80, "stop":0.7, "price_step":0.01}, {"trigger":0.85, "stop":0.7, "price_step":0.01}]}}



#{"type":"PRICE ADJUST", "order_id":"RcTSu7OcP0T7SdzHr5w3ZJ9TQ", "price":1.31 }

# {"type":"CANCEL", "order_id":"wk6z9Dr4teTRQACPWIkEwinbR", "should_liquidate":0 } #
