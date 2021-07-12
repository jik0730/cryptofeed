'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging

from cryptofeed.defines import BITFINEX_FUTURES, OPEN_INTEREST
from cryptofeed.exchanges import Bitfinex


LOG = logging.getLogger('feedhandler')


class BitfinexFutures(Bitfinex):
    id = BITFINEX_FUTURES
    symbol_endpoint = ['https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures', 'https://api-pub.bitfinex.com/v2/conf/pub:list:currency']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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
                ['pos', 
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
        open_interest = msg[1][17]
        await self.callback(OPEN_INTEREST, feed=self.id,
                            symbol=pair,
                            open_interest=Decimal(open_interest),
                            timestamp=timestamp,
                            receipt_timestamp=timestamp)