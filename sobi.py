# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader One.
#
#     Ready Trader One is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader One is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader One.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools

from typing import List

from ready_trader_one import BaseAutoTrader, Instrument, Lifespan, Side


LOT_SIZE = 100
POSITION_LIMIT = 1000
TICK_SIZE_IN_CENTS = 100


class AutoTrader(BaseAutoTrader):
    """ ASX_Bets AutoTrader
    Implementation of Static Order Book Imbalance (SOBI) order book trading
    strategy. The bot looks at the volume weighted average prices for
    bid and ask orders in the order book and determines which side
    the price will most likely swing to and places an order accordingly.

    i.e. one side isn't balanced and the price will eventually balance
    out by moving to the opposite side of the order book
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0:
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        if instrument == Instrument.ETF:

            price_adjustment = - (self.position // LOT_SIZE) * TICK_SIZE_IN_CENTS
            new_bid_price = bid_prices[0] + price_adjustment if bid_prices[0] != 0 else 0
            new_ask_price = ask_prices[0] + price_adjustment if ask_prices[0] != 0 else 0

            # get bid vwap
            total = 0
            for idx, vol in enumerate(bid_volumes):
                total += vol * bid_prices[idx]
            bid_vwap = total / sum(bid_volumes)

            # get ask vwap
            total = 0
            for idx, vol in enumerate(ask_volumes):
                total += vol * ask_prices[idx]
            ask_vwap = total / sum(ask_volumes)

            midprice = (bid_prices[0] + ask_prices[0]) / 2

            if self.bid_id != 0 and new_bid_price not in (self.bid_price, 0):
                self.send_cancel_order(self.bid_id)
                self.bid_id = 0

            if self.ask_id != 0 and new_ask_price not in (self.ask_price, 0):
                self.send_cancel_order(self.ask_id)
                self.ask_id = 0

            if self.bid_id == 0 and new_bid_price != 0 and self.position < POSITION_LIMIT:
                if abs((ask_vwap - midprice)) > abs((bid_vwap - midprice)):
                    self.bid_id = next(self.order_ids)
                    self.bid_price = new_bid_price
                    if self.position + LOT_SIZE < (POSITION_LIMIT - LOT_SIZE):
                        self.send_insert_order(self.bid_id, Side.BUY, bid_prices[0], LOT_SIZE, Lifespan.GOOD_FOR_DAY)
                        self.bids.add(self.bid_id)

            if self.ask_id == 0 and new_ask_price != 0 and self.position > -POSITION_LIMIT:
                if (abs(bid_vwap - midprice)) > (abs(ask_vwap - midprice)):
                    self.ask_id = next(self.order_ids)
                    self.ask_price = new_ask_price
                    if self.position - LOT_SIZE > (-POSITION_LIMIT + LOT_SIZE):
                        self.send_insert_order(self.ask_id, Side.SELL, ask_prices[0], LOT_SIZE, Lifespan.GOOD_FOR_DAY)
                        self.asks.add(self.ask_id)



    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when when of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        if client_order_id in self.bids:
            self.position += volume
        elif client_order_id in self.asks:
            self.position -= volume

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)