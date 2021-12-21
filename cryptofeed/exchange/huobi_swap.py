import asyncio
from collections import defaultdict
import logging
import time
from decimal import Decimal
from functools import partial
from typing import Dict, Tuple, List, Callable

import aiohttp
from yapic import json

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import HUOBI_SWAP, FUNDING, OPEN_INTEREST, LIQUIDATIONS, BUY, SELL
from cryptofeed.exchange.huobi_dm import HuobiDM
from cryptofeed.feed import Feed
from cryptofeed.standards import timestamp_normalize


LOG = logging.getLogger('feedhandler')


class HuobiSwap(HuobiDM):
    id = HUOBI_SWAP
    symbol_endpoint = ['https://api.hbdm.com/swap-api/v1/swap_contract_info', 'https://api.hbdm.com/linear-swap-api/v1/swap_contract_info']
    funding_endpoint = {
        'USD': 'https://api.hbdm.com/swap-api/v1/swap_funding_rate?contract_code=',
        'USDT': 'https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code=',
    }
    oi_endpoint = {
        'USD': 'https://api.hbdm.com/swap-api/v1/swap_open_interest?contract_code=',
        'USDT': 'https://api.hbdm.com/linear-swap-api/v1/swap_open_interest?contract_code=',
    }
    liq_endpoint = {
        'USD': 'https://api.hbdm.com/swap-api/v1/swap_liquidation_orders?contract_code=',
        'USDT': 'https://api.hbdm.com/linear-swap-api/v1/swap_liquidation_orders?contract_code=',
    }

    @classmethod
    def _parse_symbol_data(cls, data: list, symbol_separator: str) -> Tuple[Dict, Dict]:
        symbols = {}
        info = defaultdict(dict)

        for _data in data:
            for e in _data['data']:
                symbols[e['contract_code']] = e['contract_code']
                info['tick_size'][e['contract_code']] = e['price_tick']
        return symbols, info

    def __init__(self, **kwargs):
        Feed.__init__(self, {'USD': 'wss://api.hbdm.com/swap-ws', 'USDT': 'wss://api.hbdm.com/linear-swap-ws'}, **kwargs)
        self.funding_updates = {}
        self.oi_updates = {}
        self.api_max_try = 10
        self.rest_running = {'USD': False, 'USDT': False}

    async def _funding(self, pairs):
        async with aiohttp.ClientSession() as session:
            while True:
                for pair in pairs:
                    if pair[-3:] == 'USD':
                        _funding_endpoint = self.funding_endpoint['USD'] + pair
                    else:
                        _funding_endpoint = self.funding_endpoint['USDT'] + pair
                    async with session.get(_funding_endpoint) as response:
                        data = await response.text()
                        data = json.loads(data, parse_float=Decimal)

                        if data['status'] == 'ok' and 'data' in data:
                            received = time.time()
                            update = (data['data']['funding_rate'], timestamp_normalize(self.id, int(data['data']['next_funding_time'])))
                            if pair in self.funding_updates and self.funding_updates[pair] == update:
                                await asyncio.sleep(1)
                                continue
                            self.funding_updates[pair] = update
                            await self.callback(FUNDING,
                                                feed=self.id,
                                                symbol=pair,
                                                timestamp=timestamp_normalize(self.id, data['ts']),
                                                receipt_timestamp=received,
                                                rate=Decimal(update[0]),
                                                next_funding_time=update[1]
                                                )

                        await asyncio.sleep(0.1)

    async def _open_interest(self, pairs):
        async with aiohttp.ClientSession() as session:
            while True:
                for pair in pairs:
                    if pair[-3:] == 'USD':
                        _oi_endpoint = self.oi_endpoint['USD'] + pair
                    else:
                        _oi_endpoint = self.oi_endpoint['USDT'] + pair
                    async with session.get(_oi_endpoint) as response:
                        data = await response.text()
                        data = json.loads(data, parse_float=Decimal)

                        if data['status'] == 'ok' and 'data' in data:
                            received = time.time()
                            oi = data['data'][0]['amount']
                            if oi != self.oi_updates.get(pair, None):
                                self.oi_updates[pair] = oi
                                await self.callback(OPEN_INTEREST,
                                                    feed=self.id,
                                                    symbol=pair,
                                                    open_interest=Decimal(oi),
                                                    timestamp=timestamp_normalize(self.id, data['ts']),
                                                    receipt_timestamp=received
                                                    )
                        await asyncio.sleep(1)
                await asyncio.sleep(60)
    
    async def _liquidations(self, pairs):
        """
        {
            "status": "ok",
            "data": {
                "orders": [
                    {
                        "contract_code": "BTC-USD",
                        "symbol": "BTC",
                        "direction": "buy",
                        "offset": "close",
                        "volume": 173,
                        "price": 17102.9,
                        "created_at": 1606381842485,
                        "amount": 1.011524361365616357
                    }
                ],
                "total_page": 4141,
                "current_page": 1,
                "total_size": 4141
            },
            "ts": 1606381842485
        }
        """
        last_update = defaultdict(dict)

        while True:
            for pair in pairs:
                try:
                    if pair.endswith('USD'):
                        end_point = self.liq_endpoint['USD'] + pair
                    else:
                        end_point = self.liq_endpoint['USDT'] + pair
                    end_point += '&trade_type=0&create_date=7&page_size=50'

                    shortage_flag = True
                    page_index = 1
                    entries = []
                    for retry in range(self.api_max_try):
                        _end_point = end_point if len(entries) == 0 else end_point + '&page_index=' + str(page_index)
                        data = await self.http_conn.read(_end_point)
                        data = json.loads(data, parse_float=Decimal)
                        timestamp = time.time()
                        
                        if data['status'] == 'ok' and 'data' in data:
                            received = time.time()
                            if len(data['data']['orders']) == 0:
                                break
                        
                            for entry in data['data']['orders']:
                                if pair in last_update:
                                    if entry['created_at'] <= last_update.get(pair)['created_at']:
                                        shortage_flag = False
                                        break
                                else:
                                    shortage_flag = False
                                entries.append(entry)
                            page_index += 1

                            if retry == 0:  # store latest data
                                last_update[pair] = data['data']['orders'][0]
                            await asyncio.sleep(0.1)

                            if not shortage_flag:  # break if no new data
                                break

                            if shortage_flag and retry == self.api_max_try - 1:  # notify if number of retries is not enough for data shortage
                                LOG.warning("%s: Possible %s data shortage", self.id, LIQUIDATIONS)
                        else:
                            LOG.warning("%s: Possible %s data error at %s", self.id, LIQUIDATIONS, _end_point)
                            break
                            
                    for entry in entries[::-1]:  # insert oldest entry first
                        await self.callback(LIQUIDATIONS,
                                            feed=self.id,
                                            symbol=pair,
                                            side=BUY if entry['direction'] == 'buy' else SELL,
                                            leaves_qty=Decimal(entry["amount"]),
                                            price=Decimal(entry["price"]),
                                            order_id=None,
                                            timestamp=timestamp_normalize(self.id, float(entry["created_at"])),
                                            receipt_timestamp=received)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    LOG.warning("%s: Failed to get REST liquidations with possible data shortage: %s", self.id, e)

            time_to_sleep = round(60 - time.time() % 60, 6)  # call every minute
            await asyncio.sleep(time_to_sleep)

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        ret = []

        if any(pair[-4:] == 'USDT' for pair in self.normalized_symbols):
            subscribe = partial(self.subscribe, quote='USDT')
            ret.append((WSAsyncConn(self.address['USDT'], self.conn_id, **self.ws_defaults), subscribe, self.message_handler))
        if any(pair[-3:] == 'USD' for pair in self.normalized_symbols):
            subscribe = partial(self.subscribe, quote='USD')
            ret.append((WSAsyncConn(self.address['USD'], self.conn_id, **self.ws_defaults), subscribe, self.message_handler))

        return ret
    
    async def subscribe(self, conn: AsyncConnection, quote: str = None):
        if FUNDING in self.subscription:
            loop = asyncio.get_event_loop()
            pairs = self.filter_pairs(quote, FUNDING)
            loop.create_task(self._funding(self.subscription[FUNDING]))
        if OPEN_INTEREST in self.subscription:
            loop = asyncio.get_event_loop()
            pairs = self.filter_pairs(quote, OPEN_INTEREST)
            loop.create_task(self._open_interest(pairs))
        if LIQUIDATIONS in self.subscription and not self.rest_running[quote]:
            loop = asyncio.get_event_loop()
            pairs = self.filter_pairs(quote, LIQUIDATIONS)
            loop.create_task(self._liquidations(pairs))
            self.rest_running[quote] = True

        await super().subscribe(conn, quote=quote)

    def filter_pairs(self, quote: str, channel: str):
        pairs = []
        for pair in self.subscription[channel]:
            if quote == 'USDT' and pair.endswith('USDT'):
                pairs.append(pair)
            elif quote == 'USD' and pair.endswith('USD'):
                pairs.append(pair)
        return pairs
