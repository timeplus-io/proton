# Examples

This folder lists some examples to run Proton in various use cases. For more real-world examples, please check https://docs.timeplus.com/showcases

- ecommerce: a combination of Proton, Redpanda, owl-shop and Redpanda Console. Owl Shop is an imaginary ecommerce shop that simulates microservices exchanging data via Apache Kafka. Sample data streams are: clickstreams(frontend events), customer info, customer orders. [Learn more](https://docs.timeplus.com/proton-kafka#tutorial)

- carsharing: just two containers: Proton and [Chameleon](https://github.com/timeplus-io/chameleon). It is an imginary carsharing company. Sensors are equipped in each car to report car locations. The customers use the mobile app to find available cars nearby, book them, unlock them and hit the road. At the end of the trip, the customer parks the car, locks it, and ends the trip. The payment will proceed automatically with the registered credit card. [Learn more](https://docs.timeplus.com/usecases)

- hackernews: just two containers: Proton and [a bytewax-based data loader](https://github.com/timeplus-io/proton-python-driver/tree/develop/example/bytewax). Inspired by https://bytewax.io/blog/polling-hacker-news, you can call Hacker News HTTP API with Bytewax and send latest news to Proton for SQL-based analysis.
