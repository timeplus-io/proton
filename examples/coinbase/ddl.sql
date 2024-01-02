--  ddl creare stream
CREATE STREAM IF NOT EXISTS tickers (
    best_ask decimal(10,2),
    best_ask_size decimal(10,8),
    best_bid decimal(10,2),
    best_bid_size decimal(10,8),
    high_24h decimal(10,2),
    last_size decimal(10,8),
    low_24h decimal(10,2),
    open_24h decimal(10,2),
    price decimal(10,2),
    product_id string,
    sequence int,
    side string,
    time datetime,
    trade_id int,
    type string,
    volume_24h decimal(20,8),
    volume_30d decimal(20,8)
)

