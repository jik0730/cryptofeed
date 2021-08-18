from collections import defaultdict
import logging
from decimal import Decimal
from functools import partial
from typing import Dict, Tuple, Callable, List

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import GATEIO_FUTURES, TRADES, BUY, SELL, TICKER, OPEN_INTEREST
from cryptofeed.exchanges import Gateio


LOG = logging.getLogger('feedhandler')


class GateioFutures(Gateio):
    id = GATEIO_FUTURES
    symbol_endpoint = ["https://api.gateio.ws/api/v4/futures/btc/contracts", "https://api.gateio.ws/api/v4/futures/usdt/contracts"]

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
