# Examples

This folder lists some examples to run Proton in various use cases. For more real-world examples, please check https://docs.timeplus.com/showcases

- awesome-sensor-logger: You can install the free app for your phone, https://github.com/tszheichoi/awesome-sensor-logger, and use a proxy server to redirect the IoT sensor data to your local Proton, or even visualize it with Grafana.

- carsharing: just two containers: Proton and [Chameleon](https://github.com/timeplus-io/chameleon). It is an imginary carsharing company. Sensors are equipped in each car to report car locations. The customers use the mobile app to find available cars nearby, book them, unlock them and hit the road. At the end of the trip, the customer parks the car, locks it, and ends the trip. The payment will proceed automatically with the registered credit card. [Learn more](https://docs.timeplus.com/usecases)

- cdc: demonstrates how to use Debezium to sync database changes from MySQL to Proton, via Redpanda and show live updates(UPSERT and DELETE) in Proton via changelog stream.

- clickhouse: demonstrates how to read from ClickHouse or write to ClickHouse with the new External Table feature.

- ecommerce: a combination of Proton, Redpanda, owl-shop and Redpanda Console. Owl Shop is an imaginary ecommerce shop that simulates microservices exchanging data via Apache Kafka. Sample data streams are: clickstreams(frontend events), customer info, customer orders. [Learn more](https://docs.timeplus.com/proton-kafka#tutorial)

- fraud_detection: demonstrates how to leverage proton to build a real-time fraud detection where proton is used as a real-time feature store.

- grafana: an example of how to use Grafana to connect to Proton and visualize the query results.

- hackernews: just two containers: Proton and [a bytewax-based data loader](https://github.com/timeplus-io/proton-python-driver/tree/develop/example/bytewax). Inspired by https://bytewax.io/blog/polling-hacker-news, you can call Hacker News HTTP API with Bytewax and send latest news to Proton for SQL-based analysis.

- jdbc: demonstrates how to connect to Proton via JDBC using DBeaver or Metabase.

- onebrc: aggregation of 1 billion rows (of historical data) from the [1 billion row challenge](https://github.com/gunnarmorling/1brc) is fast, out-of-the-box. This is because Proton is based on ClickHouse, an open-source column-oriented DBMS for OLAP workloads. 

- Real-time-ai: build real-time RAG with open source tools: load real-time hackernews feed via Python and Bytewax, generate vector embeddings via Hugging Face Transformers, send the JSON documents to Timeplus Proton. You can apply SQL-based filter/transformation in Timeplus, then write to Kafka/Zilliz. 
