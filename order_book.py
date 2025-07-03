import math
from typing import Dict, List, Tuple, Optional, Union

class OrderBook:
    """
    A general order book class that stores bid and ask prices and sizes,
    and provides key market data.
    """

    def __init__(self, market_id: str):
        """
        Initializes an empty order book for a specific market.

        Args:
            market_id (str): A unique identifier for the market
                             (e.g., Polymarket's asset_id/market hash, Kalshi's market_ticker).
        """
        self.market_id: str = market_id
        self._bids: Dict[float, float] = {}  # Price -> Size (Bid side)
        self._asks: Dict[float, float] = {}  # Price -> Size (Ask side)
        self.last_updated_timestamp: Optional[int] = None # Unix timestamp in milliseconds

    def _update_book_level(self, side: str, price: float, size: float):
        """
        Internal helper to update a single price level in the order book.
        If size is 0 or less, the price level is removed.
        """
        if side.lower() == 'bid':
            book = self._bids
        elif side.lower() == 'ask':
            book = self._asks
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'bid' or 'ask'.")

        if size <= 0:
            if price in book:
                del book[price]
        else:
            book[price] = size

    @property
    def bids(self) -> List[Tuple[float, float]]:
        """Returns a list of (price, size) tuples for bids, sorted by price descending."""
        return sorted(self._bids.items(), key=lambda item: item[0], reverse=True)

    @property
    def asks(self) -> List[Tuple[float, float]]:
        """Returns a list of (price, size) tuples for asks, sorted by price ascending."""
        return sorted(self._asks.items(), key=lambda item: item[0])

    @property
    def highest_bid(self) -> Optional[float]:
        """Returns the highest bid price, or None if no bids."""
        return max(self._bids.keys()) if self._bids else None

    @property
    def lowest_ask(self) -> Optional[float]:
        """Returns the lowest ask price, or None if no asks."""
        return min(self._asks.keys()) if self._asks else None

    @property
    def bid_ask_spread(self) -> Optional[float]:
        """Calculates the spread between the lowest ask and highest bid."""
        if self.highest_bid is not None and self.lowest_ask is not None:
            return self.lowest_ask - self.highest_bid
        return None

    @property
    def mid_price(self) -> Optional[float]:
        """Calculates the mid-price (average of highest bid and lowest ask)."""
        if self.highest_bid is not None and self.lowest_ask is not None:
            return (self.highest_bid + self.lowest_ask) / 2
        return None

    @property
    def total_bid_liquidity(self) -> float:
        """Calculates the total size of all bids in the book."""
        return sum(self._bids.values())

    @property
    def total_ask_liquidity(self) -> float:
        """Calculates the total size of all asks in the book."""
        return sum(self._asks.values())

    @property
    def total_book_liquidity(self) -> float:
        """Calculates the sum of all bids and asks liquidity."""
        return self.total_bid_liquidity + self.total_ask_liquidity

    def get_liquidity_at_price(self, price: float, side: str) -> float:
        """
        Returns the liquidity (size) at a specific price level for a given side.
        Returns 0 if the price level does not exist.
        """
        if side.lower() == 'bid':
            return self._bids.get(price, 0.0)
        elif side.lower() == 'ask':
            return self._asks.get(price, 0.0)
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'bid' or 'ask'.")

    def get_market_depth(self, num_levels: int = 5) -> Dict[str, List[Tuple[float, float]]]:
        """
        Returns the top N bid and ask levels.

        Args:
            num_levels (int): The number of top levels to retrieve.

        Returns:
            Dict[str, List[Tuple[float, float]]]: A dictionary with 'bids' and 'asks' lists.
        """
        return {
            'bids': self.bids[:num_levels],
            'asks': self.asks[:num_levels]
        }

    def __str__(self) -> str:
        """Returns a string representation of the order book."""
        s = f"Order Book for Market: {self.market_id}\n"
        s += f"Last Updated: {self.last_updated_timestamp}\n"
        s += f"----------------------------------------\n"
        s += f"{'Price':<10} {'Size':<10} | {'Price':<10} {'Size':<10}\n"
        s += f"{'------':<10} {'------':<10} | {'------':<10} {'------':<10}\n"

        display_depth = 5 # How many levels to show in __str__

        bids_to_display = self.bids[:display_depth]
        asks_to_display = self.asks[:display_depth]

        max_len = max(len(bids_to_display), len(asks_to_display))

        for i in range(max_len):
            bid_price, bid_size = bids_to_display[i] if i < len(bids_to_display) else ("", "")
            ask_price, ask_size = asks_to_display[i] if i < len(asks_to_display) else ("", "")
            s += f"{str(bid_price):<10} {str(bid_size):<10} | {str(ask_price):<10} {str(ask_size):<10}\n"
        
        s += f"----------------------------------------\n"
        s += f"Highest Bid: {self.highest_bid:.4f} (Size: {self.get_liquidity_at_price(self.highest_bid, 'bid')})" if self.highest_bid else "Highest Bid: N/A"
        s += "\n"
        s += f"Lowest Ask:  {self.lowest_ask:.4f} (Size: {self.get_liquidity_at_price(self.lowest_ask, 'ask')})" if self.lowest_ask else "Lowest Ask: N/A"
        s += "\n"
        s += f"Spread: {self.bid_ask_spread:.4f}" if self.bid_ask_spread is not None else "Spread: N/A"
        s += "\n"
        s += f"Mid-Price: {self.mid_price:.4f}" if self.mid_price is not None else "Mid-Price: N/A"
        s += "\n"
        s += f"Total Bid Liquidity: {self.total_bid_liquidity:.2f}\n"
        s += f"Total Ask Liquidity: {self.total_ask_liquidity:.2f}\n"
        s += f"Total Book Liquidity: {self.total_book_liquidity:.2f}\n"
        return s