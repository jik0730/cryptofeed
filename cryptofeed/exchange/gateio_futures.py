import asyncio
from collections import defaultdict
import logging
import time
from decimal import Decimal
from functools import partial
from typing import Dict, Tuple, Callable, List
from yapic import json

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import GATEIO_FUTURES, TRADES, BUY, SELL, TICKER, OPEN_INTEREST, LIQUIDATIONS
from cryptofeed.exchanges import Gateio


LOG = logging.getLogger('feedhandler')


class GateioFutures(Gateio):
    id = GATEIO_FUTURES
    api = "https://api.gateio.ws/api/v4/"
    symbol_endpoint = ["https://api.gateio.ws/api/v4/futures/btc/contracts", "https://api.gateio.ws/api/v4/futures/usdt/contracts"]
    api_max_try = 10

    @classmethod
    def _parse_symbol_data(cls, data_list: list, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for data in data_list:
            for _data in data:
                if not _data['in_delisting']:
                    ret[_data['name'].replace('_', symbol_separator)] = _data['name']
        return ret, info

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.address = {'USD': 'wss://fx-ws.gateio.ws/v4/ws/btc', 'USDT': 'wss://fx-ws.gateio.ws/v4/ws/usdt'}
        self.rest_running = False

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []

        if any(pair[-4:] == 'USDT' for pair in self.normalized_symbols):
            subscribe = partial(self.subscribe, quote='USDT')
            ret.append((WSAsyncConn(self.address['USDT'], self.conn_id, **self.ws_defaults), subscribe, self.message_handler))
        if any(pair[-3:] == 'USD' for pair in self.normalized_symbols):
            subscribe = partial(self.subscribe, quote='USD')
            ret.append((WSAsyncConn(self.address['USD'], self.conn_id, **self.ws_defaults), subscribe, self.message_handler))

        return ret

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            "time":1541659086,
            "channel":"futures.tickers",
            "event":"update",
            "error":null,
            "result":[
                {
                    "contract":"BTC_USD",
                    "last":"118.4",
                    "change_percentage":"0.77",
                    "funding_rate":"-0.000114",
                    "funding_rate_indicative": "0.01875",
                    "mark_price":"118.35",
                    "index_price":"118.36",
                    "total_size":"73648",
                    "volume_24h":"745487577",
                    "volume_24h_btc" : "117",
                    "volume_24h_usd" : "419950",
                    "quanto_base_rate":"",
                    "volume_24h_quote": "1665006",
                    "volume_24h_settle": "178",
                    "volume_24h_base": "5526"
                }
            ]
        }
        """
        # NOTE ticker is not implemented
        data = msg['result']
        for ticker in data:
            await self.callback(OPEN_INTEREST, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(ticker['contract']),
                                open_interest=Decimal(ticker['total_size']),
                                timestamp=float(msg['time']),
                                receipt_timestamp=timestamp)

    async def _trades(self, msg: dict, timestamp: float):
        """
        {
            "channel": "futures.trades",
            "event": "update",
            "time": 1541503698,
            "result": [
                {
                "size": -108,
                // + taker is buyer，- taker is seller
                "id": 27753479,
                "create_time": 1545136464,
                "create_time_ms": 1545136464123,
                "price": "96.4",
                "contract": "BTC_USD"
                }
            ]
        }
        """
        data = msg['result']
        for trade in data:
            await self.callback(TRADES, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(trade['contract']),
                                side=SELL if trade['size'] < 0 else BUY,
                                amount=Decimal(abs(trade['size'])),
                                price=Decimal(trade['price']),
                                timestamp=float(trade['create_time_ms']) / 1000,
                                receipt_timestamp=timestamp,
                                order_id=trade['id'])

    async def _liquidations(self, pairs: list):
        last_update = defaultdict(dict)
        """
        [
            {
                "time": 1548654951,
                "contract": "BTC_USDT",
                "size": 600,
                "leverage": "25",
                "margin": "0.006705256878",
                "entry_price": "3536.123",
                "liq_price": "3421.54",
                "mark_price": "3420.27",
                "order_id": 317393847,
                "order_price": "3405",
                "fill_price": "3424",
                "left": 0
            }
        ]
        """

        while True:
            for pair in pairs:
                try:
                    if pair.endswith('USDT'):
                        settle = 'usdt'
                    else:
                        settle = 'btc'
                    end_point = f"{self.api}futures/{settle}/liq_orders?contract={pair}&limit=100"

                    shortage_flag = True
                    entries = []
                    for retry in range(self.api_max_try):
                        _end_point = end_point if len(entries) == 0 else end_point + '&to=' + str(data[-1]['time'])
                        data = await self.http_conn.read(_end_point)
                        data = json.loads(data, parse_float=Decimal)
                        timestamp = time.time()
                        if len(data) == 0:
                            break
                        
                        for entry in data:
                            if pair in last_update:
                                if entry['time'] <= last_update.get(pair)['time']:
                                    shortage_flag = False
                                    break
                            else:
                                shortage_flag = False
                            entries.append(entry)

                        if retry == 0:  # store latest data
                            last_update[pair] = data[0]
                        await asyncio.sleep(0.1)

                        if not shortage_flag:  # break if no new data
                            break

                        if shortage_flag and retry == self.api_max_try - 1:  # notify if number of retries is not enough for data shortage
                            LOG.warning("%s: Possible %s data shortage", self.id, LIQUIDATIONS)
                        
                    for entry in entries[::-1]:  # insert oldest entry first
                        await self.callback(LIQUIDATIONS,
                                            feed=self.id,
                                            symbol=self.exchange_symbol_to_std_symbol(pair),
                                            side=BUY if entry['size'] < 0 else SELL,
                                            leaves_qty=Decimal(abs(entry["size"])),
                                            price=Decimal(entry["fill_price"]),
                                            order_id=None,
                                            timestamp=entry["time"],
                                            receipt_timestamp=timestamp)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    LOG.warning("%s: Failed to get REST liquidations with possible data shortage: %s", self.id, e)

            time_to_sleep = round(60 - time.time() % 60, 6)  # call every minute
            await asyncio.sleep(time_to_sleep)

    async def subscribe(self, conn: AsyncConnection, quote: str = None):
        if LIQUIDATIONS in self.subscription and not self.rest_running:
            loop = asyncio.get_event_loop()
            pairs = []
            for pair in self.subscription[LIQUIDATIONS]:
                if quote == 'USDT' and pair.endswith('USDT'):
                    pairs.append(pair)
                elif quote == 'USD' and pair.endswith('USD'):
                    pairs.append(pair)
            loop.create_task(self._liquidations(pairs))
            self.rest_running = True

        await super().subscribe(conn, quote=quote)
