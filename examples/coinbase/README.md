# Demo for Benthos data pipeline and Coinbase websocket data



This docker compose file demonstrates how to ingest websocket data into proton by using benthos pipeline. 



## Start the stack

Simply run `docker compose up` in this folder. Three docker containers in the stack:

1. ghcr.io/timeplus-io/proton:latest, as the streaming database
2. jeffail/benthos:latest, a [benthos](https://www.benthos.dev/) service as the data pipeline
3. init container, create the tickers stream when proton database server is ready

the ddl to create the stream is:

```sql
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
```

## Create a ingest data pipeline

run command `make create` to create following benthos data pipeline, note you need install `jq` and `curl` to run this command

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

now you can run following query to get the OHLC of the crypto data

```sql
SELECT
  window_start, product_id, earliest(price) AS open, max(price) AS high, min(price) AS low, latest(price) AS close, latest(volume_24h) as acc_vol
FROM
  tumble(tickers, 60s)
WHERE
  product_id != '' and _tp_time > earliest_ts()
GROUP BY
  window_start, product_id
```


