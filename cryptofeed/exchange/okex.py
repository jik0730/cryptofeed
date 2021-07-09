'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from collections import defaultdict
from decimal import Decimal
import logging
import time
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import OKEX, BUY, SELL, TRADES, OPEN_INTEREST
from cryptofeed.feed import Feed
from cryptofeed.standards import timestamp_normalize


LOG = logging.getLogger("feedhandler")


class OKEx(Feed):
    id = OKEX
    api = 'https://www.okex.com/api/'
    symbol_endpoint = ['https://www.okex.com/api/v5/public/instruments?instType=SPOT', 'https://www.okex.com/api/v5/public/instruments?instType=SWAP', 'https://www.okex.com/api/v5/public/instruments?instType=FUTURES']

    @classmethod
    def _parse_symbol_data(cls, data: list, symbol_separator: str) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for entry in data:
            for e in entry["data"]:
                ret[e['instId'].replace("-", symbol_separator)] = e['instId']
                info['tickSz'][e['instId']] = e['tickSz']
        return ret, info

    def __init__(self, **kwargs):
        super().__init__('wss://ws.okex.com:8443/ws/v5/public', **kwargs)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {"arg":{"channel":"trades","instId":"BTC-USD-SWAP"},"data":[{"instId":"BTC-USD-SWAP","tradeId":"129031616","px":"32885.6","sz":"2","side":"sell","ts":"1625798415799"}]}
        """
        for trade in msg['data']:
            await self.callback(TRADES,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(trade['instId']),
                                order_id=trade['tradeId'],
                                side=BUY if trade['side'] == 'buy' else SELL,
                                amount=Decimal(trade['sz']),
                                price=Decimal(trade['px']),
                                timestamp=timestamp_normalize(self.id, int(trade['ts'])),
                                receipt_timestamp=timestamp
                                )

    async def _open_interest(self, msg: dict, timestamp: float):
        for oi in msg['data']:
            await self.callback(OPEN_INTEREST,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(oi['instId']),
                                open_interest=Decimal(oi['oi']),
                                timestamp=timestamp_normalize(self.id, int(oi['ts'])),
                                receipt_timestamp=timestamp
                                )

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'event' in msg:
            if msg['event'] == 'error':
                LOG.error("%s: Error: %s", self.id, msg)
            elif msg['event'] == 'subscribe':
                pass
            else:
                LOG.warning("%s: Unhandled event %s", self.id, msg)
        elif 'data' in msg:
            if msg['arg']['channel'] == 'trades':
                await self._trade(msg, timestamp)
            elif msg['arg']['channel'] == 'open-interest':
                await self._open_interest(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled message %s", self.id, msg)
        else:
            LOG.warning("%s: Unhandled message %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        for chan in self.subscription:
            for pair in self.subscription[chan]:
                pair = self.exchange_symbol_to_std_symbol(pair)
                await conn.write(json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": chan,
                                "instId": pair
                            }
                        ]
                    }
                ))