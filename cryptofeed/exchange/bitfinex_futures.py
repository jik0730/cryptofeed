'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging

from cryptofeed.defines import BITFINEX_FUTURES, OPEN_INTEREST, LIQUIDATIONS, SELL, BUY
from cryptofeed.exchanges import Bitfinex
from cryptofeed.standards import timestamp_normalize


LOG = logging.getLogger('feedhandler')


class BitfinexFutures(Bitfinex):
    id = BITFINEX_FUTURES
    symbol_endpoint = ['https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures', 'https://api-pub.bitfinex.com/v2/conf/pub:list:currency']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.exchange_symbols = [self.std_symbol_to_exchange_symbol(symbol) for symbol in self.symbols]

    async def _status(self, pair: str, msg: list, timestamp: float):
        """
        // Derivatives Status
        [
            CHANNEL_ID,
            [
                TIME_MS,
                PLACEHOLDER, 
                DERIV_PRICE,
                SPOT_PRICE,
                PLACEHOLDER,
                INSURANCE_FUND_BALANCE,
                PLACEHOLDER,
                NEXT_FUNDING_EVT_TIMESTAMP_MS,
                NEXT_FUNDING_ACCRUED,
                NEXT_FUNDING_STEP,
                PLACEHOLDER,
                CURRENT_FUNDING
                PLACEHOLDER,
                PLACEHOLDER,
                MARK_PRICE,
                PLACEHOLDER,
                PLACEHOLDER,
                OPEN_INTEREST,
                PLACEHOLDER,
                PLACEHOLDER,
                PLACEHOLDER,
                CLAMP_MIN,
                CLAMP_MAX
            ]
        ]

        // Liquidation feed updates
        [
            CHANNEL_ID,
            [
                [
                    'pos', 
                    POS_ID, 
                    TIME_MS, 
                    PLACEHOLDER, 
                    SYMBOL, 
                    AMOUNT, 
                    BASE_PRICE, 
                    PLACEHOLDER, 
                    IS_MATCH, 
                    IS_MARKET_SOLD, 
                    PLACEHOLDER
                    LIQUIDATION_PRICE
                ]
            ]
        ]
        """
        if msg[1] == 'hb':
            return  # ignore heartbeats
        
        if isinstance(msg[1][0], list):  # liquidation
            symbol = msg[1][0][4]
            if symbol in self.exchange_symbols:
                side = BUY if msg[1][0][5] < 0 else SELL
                pair = self.exchange_symbol_to_std_symbol(symbol)
                await self.callback(LIQUIDATIONS,
                                    feed=self.id,
                                    symbol=pair,
                                    side=side,
                                    leaves_qty=Decimal(abs(msg[1][0][5])),
                                    price=Decimal(msg[1][0][6]),
                                    order_id=msg[1][0][1],
                                    timestamp=timestamp_normalize(self.id, msg[1][0][2]),
                                    receipt_timestamp=timestamp)
        else:  # ticker
            open_interest = msg[1][17] * msg[1][2]  # USD
            await self.callback(OPEN_INTEREST, feed=self.id,
                                symbol=pair,
                                open_interest=Decimal(open_interest),
                                timestamp=timestamp_normalize(self.id, msg[1][0]),
                                receipt_timestamp=timestamp)