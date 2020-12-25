'''
Copyright (C) 2017-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from typing import Tuple, Callable, Union, List

from cryptofeed.callback import Callback
from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import (ASK, BID, BOOK_DELTA, FUNDING, FUTURES_INDEX, L2_BOOK, L3_BOOK, LIQUIDATIONS,
                                OPEN_INTEREST, MARKET_INFO, TICKER, TRADES, TRANSACTIONS, VOLUME)
from cryptofeed.exceptions import BidAskOverlapping, UnsupportedDataFeed
from cryptofeed.standards import feed_to_exchange, get_exchange_info, load_exchange_pair_mapping, pair_std_to_exchange
from cryptofeed.util.book import book_delta, depth


class Feed:
    id = 'NotImplemented'

    def __init__(self, address: Union[dict, str], pairs=None, channels=None, config=None, callbacks=None, max_depth=None, book_interval=1000, snapshot_interval=False, checksum_validation=False, cross_check=False, origin=None, key_id=None):
        """
        max_depth: int
            Maximum number of levels per side to return in book updates
        book_interval: int
            Number of updates between snapshots. Only applicable when book deltas are enabled.
            Book deltas are enabled by subscribing to the book delta callback.
        snapshot_interval: bool/int
            Number of updates between snapshots. Only applicable when book delta is not enabled.
            Updates between snapshots are not delivered to the client
        checksum_validation: bool
            Toggle checksum validation, when supported by an exchange.
        cross_check: bool
            Toggle a check for a crossed book. Should not be needed on exchanges that support
            checksums or provide message sequence numbers.
        origin: str
            Passed into websocket connect. Sets the origin header.
        key_id: str
            API key to query the feed, required when requesting supported coins/pairs.
        """
        self.config = defaultdict(set)
        self.address = address
        self.book_update_interval = book_interval
        self.snapshot_interval = snapshot_interval
        self.cross_check = cross_check
        self.updates = defaultdict(int)
        self.do_deltas = False
        self.pairs = []
        self.normalized_pairs = []
        self.channels = []
        self.max_depth = max_depth
        self.previous_book = defaultdict(dict)
        self.origin = origin
        self.checksum_validation = checksum_validation
        self.ws_defaults = {'ping_interval': 10, 'ping_timeout': None, 'max_size': 2**23, 'max_queue': None, 'origin': self.origin}
        load_exchange_pair_mapping(self.id, key_id=key_id)

        if config is not None and (pairs is not None or channels is not None):
            raise ValueError("Use config, or channels and pairs, not both")

        if config is not None:
            for channel in config:
                chan = feed_to_exchange(self.id, channel)
                self.config[chan].update([pair_std_to_exchange(pair, self.id) for pair in config[channel]])
                self.normalized_pairs.extend(self.config[chan])

        if pairs:
            self.normalized_pairs = pairs
            self.pairs = [pair_std_to_exchange(pair, self.id) for pair in pairs]
        if channels:
            self.channels = list(set([feed_to_exchange(self.id, chan) for chan in channels]))

        self.l3_book = {}
        self.l2_book = {}
        self.callbacks = {FUNDING: Callback(None),
                          FUTURES_INDEX: Callback(None),
                          L2_BOOK: Callback(None),
                          L3_BOOK: Callback(None),
                          LIQUIDATIONS: Callback(None),
                          OPEN_INTEREST: Callback(None),
                          MARKET_INFO: Callback(None),
                          TICKER: Callback(None),
                          TRADES: Callback(None),
                          TRANSACTIONS: Callback(None),
                          VOLUME: Callback(None)
                          }

        if callbacks:
            for cb_type, cb_func in callbacks.items():
                self.callbacks[cb_type] = cb_func
                if cb_type == BOOK_DELTA:
                    self.do_deltas = True

        for key, callback in self.callbacks.items():
            if not isinstance(callback, list):
                self.callbacks[key] = [callback]

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        """
        Generic connection method for exchanges. Exchanges that require/support
        multiple addresses will need to override this method in their specific class
        unless they use the same subscribe method and message handler for all
        connections.

        Connect returns a list of tuples. Each tuple contains
        1. an AsyncConnection object
        2. the subscribe function pointer associated with this connection
        3. the message handler for this connection
        """
        ret = []

        if isinstance(self.address, str):
            return [(AsyncConnection(self.address, self.id, **self.ws_defaults), self.subscribe, self.message_handler)]

        for key, addr in self.address.items():
            ret.append((AsyncConnection(addr, self.id, **self.ws_defaults), self.subscribe, self.message_handler))
        return ret

    @classmethod
    def info(cls, key_id: str = None) -> dict:
        """
        Return information about the Exchange - what trading pairs are supported, what data channels, etc

        key_id: str
            API key to query the feed, required when requesting supported coins/pairs.
        """
        pairs, info = get_exchange_info(cls.id, key_id=key_id)
        data = {'pairs': list(pairs.keys()), 'channels': []}
        for channel in (FUNDING, FUTURES_INDEX, LIQUIDATIONS, L2_BOOK, L3_BOOK, OPEN_INTEREST, MARKET_INFO, TICKER, TRADES, TRANSACTIONS, VOLUME):
            try:
                feed_to_exchange(cls.id, channel, silent=True)
                data['channels'].append(channel)
            except UnsupportedDataFeed:
                pass

        data.update(info)
        return data

    async def book_callback(self, book: dict, book_type: str, pair: str, forced: bool, delta: dict, timestamp: float, receipt_timestamp: float):
        """
        Three cases we need to handle here

        1.  Book deltas are enabled (application of max depth here is trivial)
        1a. Book deltas are enabled, max depth is not, and exchange does not support deltas. Rare
        2.  Book deltas not enabled, but max depth is enabled
        3.  Neither deltas nor max depth enabled
        4.  Book deltas disabled and snapshot intervals enabled (with/without max depth)

        2 and 3 can be combined into a single block as long as application of depth modification
        happens first

        For 1, need to handle separate cases where a full book is returned vs a delta
        """
        if self.do_deltas:
            if not forced and self.updates[pair] < self.book_update_interval:
                if self.max_depth:
                    delta, book = await self.apply_depth(book, True, pair)
                    if not (delta[BID] or delta[ASK]):
                        return
                elif not delta:
                    # this will only happen in cases where an exchange does not support deltas and max depth is not enabled.
                    # this is an uncommon situation. Exchanges that do not support deltas will need
                    # to populate self.previous internally to avoid the unncesessary book copy on all other exchanges
                    delta = book_delta(self.previous_book[pair], book, book_type=book_type)
                    if not (delta[BID] or delta[ASK]):
                        return
                self.updates[pair] += 1
                if self.cross_check:
                    self.check_bid_ask_overlapping(book, pair)
                await self.callback(BOOK_DELTA, feed=self.id, pair=pair, delta=delta, timestamp=timestamp, receipt_timestamp=receipt_timestamp)
                if self.updates[pair] != self.book_update_interval:
                    return
            elif forced and self.max_depth:
                # We want to send a full book update but need to apply max depth first
                _, book = await self.apply_depth(book, False, pair)
        elif self.max_depth:
            if not self.snapshot_interval or (self.snapshot_interval and self.updates[pair] >= self.snapshot_interval):
                changed, book = await self.apply_depth(book, False, pair)
                if not changed:
                    return
        # case 4 - incremement skiped update, and exit
        if self.snapshot_interval and self.updates[pair] < self.snapshot_interval:
            self.updates[pair] += 1
            return

        if self.cross_check:
            self.check_bid_ask_overlapping(book, pair)
        if book_type == L2_BOOK:
            await self.callback(L2_BOOK, feed=self.id, pair=pair, book=book, timestamp=timestamp, receipt_timestamp=receipt_timestamp)
        else:
            await self.callback(L3_BOOK, feed=self.id, pair=pair, book=book, timestamp=timestamp, receipt_timestamp=receipt_timestamp)
        self.updates[pair] = 0

    def check_bid_ask_overlapping(self, book, pair):
        bid, ask = book[BID], book[ASK]
        if len(bid) > 0 and len(ask) > 0:
            best_bid, best_ask = bid.keys()[-1], ask.keys()[0]
            if best_bid >= best_ask:
                raise BidAskOverlapping(f"{self.id} {pair} best bid {best_bid} >= best ask {best_ask}")

    async def callback(self, data_type, **kwargs):
        for cb in self.callbacks[data_type]:
            await cb(**kwargs)

    async def apply_depth(self, book: dict, do_delta: bool, pair: str):
        ret = depth(book, self.max_depth)
        if not do_delta:
            delta = self.previous_book[pair] != ret
            self.previous_book[pair] = ret
            return delta, ret

        delta = book_delta(self.previous_book[pair], ret)
        self.previous_book[pair] = ret
        return delta, ret

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        raise NotImplementedError

    async def subscribe(self, connection: AsyncConnection, **kwargs):
        """
        kwargs will not be passed from anywhere, if you need to supply extra data to
        your subscribe, bind the data to the method with a partial
        """
        raise NotImplementedError

    async def stop(self):
        for callbacks in self.callbacks.values():
            for callback in callbacks:
                if hasattr(callback, 'stop'):
                    await callback.stop()
