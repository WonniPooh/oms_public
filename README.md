# Binance Trading Order Management Software

Software for managing position. Has API so can be connected to any kind of custom software.

> [!IMPORTANT]
> To make code work you need to manually replace in python-binance module:
> <br />```self._loop: asyncio.AbstractEventLoop = get_loop()```
> <br />to
> <br />```self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop() ```
> <br />in file https://github.com/sammchardy/python-binance/blob/master/binance/threaded_stream.py


At the moment supports connection to Binance FUTURES and can process  such orders:

<br />1. Market
<br />2. Limit
<br />3. Stop (Both market and limit)
<br />4. Custom OCO - Stop order and limit are exchanged depending on whether price is closer to limit or stop. Switch happens when price reaches 30% area (if total price delta between limit and stop order is considered as 100%)

Soon will be added Trailing order and Combined order -both custom functionality as long as a GUI tool to manually test provided functionality.

Connection happens through websoket interface on "127.0.0.1", port 8877

API has different command types:
<br />-SWITCH PRICE SOURCE - allows to stop/start price update from exchange and pass custom prices, allows for example to test orders switch in OCO order
<br />-PRICE ADJUST - send custom price
<br />-GET_INFO - allows to get some info, such as balance state
<br />-CANCEL - allows to cancel order
<br />-NEW ORDER - allows to place new order

Available order types:
<br />-MARKET
<br />-LIMIT
<br />-STOP_MARKET
<br />-STOP_LIMIT
<br />-CUSTOM_OCO
<br />-TRAILING_STOP (not ready yet)
<br />-COMBINED (not ready yet)

Custom **order_id** could be provided that allows to interact with order.  

Example API call for Market order placed:
> {"username": "Worker", "type": "NEW ORDER", "order_id": "nDdmEsrPPC", "account_type": "FUTURES", "order_type": "MARKET", "symbol": "ADAUSDT", "side": "BUY", "quote": 20.0, "leverage": "2", "is_repay": false}

- side: BUY | SELL

- is_repay: if True then we only reduce out position

- quote - amount of USDT to spent (total -  margin used * leverage, so if leverage is 2, 10$ margin will be used) symbol for ADAUSDT, as an example

- username is set in users.json file which holds API keys


Example API call for Limit order placed:
>{"username": "Worker", "type": "NEW ORDER", "order_id": "olu1t77UJR", "account_type": "FUTURES", "order_type": "LIMIT", "symbol": "ADAUSDT", "side": "BUY", "quote": 20.0, "action_price": "0.55", "leverage": "2", "is_repay": false}

Example API call for StopMarket order placed:
>{"username": "Worker", "type": "NEW ORDER", "order_id": "tKI0NJmwAJ", "account_type": "FUTURES", "order_type": "STOP_MARKET", "symbol": "ADAUSDT", "side": "BUY", "quote": 20.0, "trigger_price": "0.55", "leverage": "2", "is_repay": false}

Example API call for StopLimit order placed:
>{"username": "Worker", "type": "NEW ORDER", "order_id": "mMTnxpGPUy", "account_type": "FUTURES", "order_type": "STOP_LIMIT", "symbol": "ADAUSDT", "side": "BUY", "quote": 20.0, "trigger_price": "0.55", "leverage": "2", "is_repay": false, "action_price": "0.55"}

Example API call for OCO order placed:
>{"username": "Alex", "type": "NEW ORDER", "order_id": "OeDh720mar", "account_type": "FUTURES", "order_type": "CUSTOM_OCO", "symbol": "ADAUSDT", "side": "SELL", "quote": 20.0, "stop_type": "STOP_LIMIT", "limit_price": "0.55", "trigger_price": "0.55", "leverage": "2", "is_repay": false, "action_price": "0.55"}

-stop_type is a type of stop order, can be **STOP_LIMIT** or **STOP_MARKET**. For **STOP_MARKET** "action_price" param is not required

Example API CANCEL order:
> {"username": "Worker", "type":"CANCEL", "account_type":"FUTURES", "order_id":order_id, "should_liquidate":true }

Example API call of manual price change for some exact order:
> {"username": "Worker", "type":"PRICE ADJUST", "account_type":"FUTURES", "order_id":order_id, "price":new_price}

Example API call for switch price source command:
>{"username": "Worker", "type": "SWITCH PRICE SOURCE"}

> [!CAUTION]
> This project is for informational purposes only. You should not construe any such information or other material as legal, tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, >recommendation, endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial instruments in this or in any other jurisdiction in which such solicitation or offer would be >unlawful under the securities laws of such jurisdiction.
> 
>If you plan to use real money, USE AT YOUR OWN RISK.
>
>Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs, or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
