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
from cryptofeed.defines import OKEX, BUY, SELL, TRADES, OPEN_INTEREST, LIQUIDATIONS, FILLED, UNFILLED
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

    async def _liquidations(self, pairs: list):
        last_update = defaultdict(dict)
        """
        for PERP liquidations, the following arguments are required: uly, state
        for FUTURES liquidations, the following arguments are required: uly, state, alias
        FUTURES, MARGIN and OPTION liquidation request not currently supported by the below
        """

        while True:
            for pair in pairs:
                if 'SWAP' in pair:
                    instrument_type = 'SWAP'
                    uly = pair.split("-")[0] + "-" + pair.split("-")[1]
                else:
                    continue

                for status in (FILLED, UNFILLED):
                    end_point = f"{self.api}v5/public/liquidation-orders?instType={instrument_type}&limit=100&state={status}&uly={uly}"
                    data = await self.http_conn.read(end_point)
                    data = json.loads(data, parse_float=Decimal)
                    timestamp = time.time()
                    if len(data['data'][0]['details']) == 0 or (len(data['data'][0]['details']) > 0 and last_update.get(pair) == data['data'][0]['details'][0]):
                        continue
                    
                    shortage_flag = True
                    for entry in data['data'][0]['details']:
                        if pair in last_update:
                            if entry == last_update[pair].get(status):
                                shortage_flag = False
                                break
                        await self.callback(LIQUIDATIONS,
                                            feed=self.id,
                                            symbol=pair,
                                            side=BUY if entry['side'] == 'buy' else SELL,
                                            leaves_qty=Decimal(entry["sz"]),
                                            price=Decimal(entry["bkPx"]),
                                            order_id=None,
                                            timestamp=timestamp_normalize(self.id, float(entry["ts"])),
                                            receipt_timestamp=timestamp)
                    if pair in last_update and shortage_flag:
                        LOG.warning("%s: Possible %s data shortage", self.id, LIQUIDATIONS)
                    last_update[pair][status] = data['data'][0]['details'][0]
                await asyncio.sleep(0.1)
            
            time_to_sleep = round(60 - time.time() % 60, 6)  # call every minute
            await asyncio.sleep(time_to_sleep)

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
        if LIQUIDATIONS in self.subscription:
            loop = asyncio.get_event_loop()
            loop.create_task(self._liquidations(self.subscription[LIQUIDATIONS]))

        for chan in self.subscription:
            if chan in [LIQUIDATIONS]:
                continue
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