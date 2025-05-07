import random

# TODO: Add API call to coinmarketcap or coingecko API to get historical and current pricing data

def mock_fetch_price(ticker, date):
    # Simulate getting historical and current prices
    past_price = round(random.uniform(0.1, 5.0), 2)
    # Simulate realistic variation
    current_price = round(past_price * random.uniform(0.5, 2.0), 2)
    return past_price, current_price