--  ddl creare stream
CREATE STREAM IF NOT EXISTS tickers (
    best_ask float64,
    best_ask_size float64,
    best_bid float64,
    best_bid_size float64,
    high_24h float64,
    last_size float64,
    low_24h float64,
    open_24h float64,
    price float64,
    product_id string,
    sequence int,
    side string,
    time datetime,
    trade_id int,
    type string,
    volume_24h float64,
    volume_30d float64
)

