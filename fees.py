import math

# Define Polymarket fee (currently zero)
POLYMARKET_FEE_PERCENT = 0.0

def calculate_kalshi_fee(trade_size: float, price: float) -> float:
    """
    Calculates the Kalshi trading fee based on their formula.
    The fee is 7 cents per contract on the notional value of a trade, where notional
    value is calculated as quantity * price * (1 - price).
    The fee is rounded up to the nearest cent.

    Args:
        trade_size: The number of contracts.
        price: The execution price per contract (e.g., 0.50 for 50 cents).

    Returns:
        The total fee in dollars.
    """
    if trade_size <= 0 or price <= 0 or price >= 1:
        return 0.0
    
    # Calculate fee in cents based on the official formula
    fee_in_cents = math.ceil(0.07 * trade_size * price * (1.0 - price) * 100)
    
    # Return fee in dollars
    return fee_in_cents / 100.0