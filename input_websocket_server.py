import queue
from common import *

should_be_canceled = False
tcp_connections_count = 0

websocket_handlers = {}
clients = {}

import asyncio
import json
import websockets

def run_socket_server():
    logger = get_logger('default')
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    socket_server = websockets.serve(handle_new_connection, "127.0.0.1", 8877)
    logger.info(compose_log_msg(getframeinfo(currentframe()),
                                "socket server connection open"))
    event_loop.run_until_complete(socket_server)

    event_loop.run_forever()

    logger.info(compose_log_msg(getframeinfo(currentframe()),
                                "socket server connection closed"))


async def forward_data_to_user(websocket, acc_queue):
    loop = asyncio.get_event_loop()
    logger = get_logger('default')
    fail_counter = 0

    while True:
        data = await loop.run_in_executor(None, acc_queue.get)
        print(f"New data for the user: {data}")
        while True:
            try:
                await websocket.send(data)
                break
            except Exception as e:
                handle_exception(logger, e)
                print("Failure on sending data:", data)
                fail_counter += 1

                if fail_counter > 5:
                    logger.error("WS failed to forward data. Killing WS sender")
                    return

async def handle_new_connection(websocket, path):
    logger = get_logger('default')
    logger.info(compose_log_msg(getframeinfo(currentframe()),
                                "New socket connection"))

    try:
        unparsed_data = await websocket.recv()
        parsed = json.loads(unparsed_data)

        #TODO check running managers b4
        acc_username = parsed.get("username")
        account_manager = websocket_handlers["managers"].get(acc_username)

        if account_manager is None:
            await websocket.send("Requested account not found")
            return

        task = asyncio.create_task(forward_data_to_user(websocket, account_manager.out_ws_queue))
        await websocket.send("Success connection")

        while True:
            request = await websocket.recv()

            decrypted_cmd = json.loads(request)

            #TODO split root vs client CMDs
            if "cmd" in decrypted_cmd:
                cmd = decrypted_cmd.get("cmd").lower()
                if "ping" == cmd:
                    await websocket.send("pong")
                    continue

                if "exit" == cmd:
                    break

                if "update_accounts" == cmd:
                    await websocket_handlers["update_accounts"]()
                    continue

                if "shut_down" == cmd:
                    await websocket_handlers["shut_down"]()

                    global should_be_canceled
                    should_be_canceled = True
                    return

            print("handle_new_connection", decrypted_cmd)

            parsed_cmd = decrypted_cmd
            acc_username = parsed_cmd.get("username")

            if acc_username is None:
                await websocket.send("Missing account name")

            if acc_username == "all":
                for account in websocket_handlers["managers"].values():
                    asyncio.run_coroutine_threadsafe(account.process_new_cmd(parsed_cmd), account.loop)
            else:
                account = websocket_handlers["managers"].get(acc_username)
                if account is None:
                    await websocket.send(f"No account with name {acc_username}")
                    logger.error(compose_log_msg(getframeinfo(currentframe()),
                                                 f"New cmd for non-existed account {acc_username} : {parsed_cmd}"))
                else:
                    asyncio.run_coroutine_threadsafe(account.process_new_cmd(parsed_cmd), account.loop)

    except Exception as e:
        handle_exception(logger, e)
    finally:
        logger.info(compose_log_msg(getframeinfo(currentframe()),
                                    "Closed socket connection"))

