# Demo for Benthos data pipeline and Coinbase websocket data



This docker compose file demonstrates how to ingest WebSocket data into Proton by using Benthos pipeline.



## Start the stack

Simply run `docker compose up` in this folder. Three docker containers in the stack:

1. d.timeplus.com/timeplus-io/proton:latest, as the streaming database
2. jeffail/benthos:latest, a [Benthos](https://www.benthos.dev/) service as the data pipeline
3. init container, create the tickers stream when Proton database server is ready

the ddl to create the stream is:

```sql
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
```

## Create a ingest data pipeline

following ingest pipeline will be created by `init-pipeline` container

```
input:
  label: coinbase
  websocket:
    url:  wss://ws-feed.exchange.coinbase.com
    open_message: '{"type": "subscribe","product_ids": ["ETH-USD","ETH-EUR"],"channels": ["ticker"]}'
    open_message_type: text

output:
  http_client:
    url: http://proton:8123/proton/v1/ingest/streams/tickers
    verb: POST
    headers:
      Content-Type: application/json
    batching:
      count: 10
      period: 1000ms
      processors:
        - archive:
            format: json_array
        - mapping: |
            root.columns = this.index(0).keys()
            root.data = this.map_each( row -> root.columns.map_each( key -> row.get(key)) )

```

this pipeline will read data from coinbase websocket and then send the result to proton ingest api in a batch


## Query you crypto price data with SQL

now you can run following query to get the OHLC of the crypto data:

```sql
SELECT
  window_start, product_id, earliest(price) AS open, max(price) AS high, min(price) AS low, latest(price) AS close
FROM
  tumble(tickers, 60s)
WHERE
  product_id != '' and _tp_time > earliest_ts()
GROUP BY
  window_start, product_id
```
