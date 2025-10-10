# Example to Query Kafka Data with SQL and Visualize Results Using Marimo
👋 This is a live notebook, built with [Timeplus](https://github.com/timeplus-io/proton) and [marimo](https://marimo.io), showing streaming data from GitHub via a public facing Kafka broker.

Simply run the following commands:
```bash
curl https://astral.sh/uv/install.sh | sh
curl https://install.timeplus.com/oss | sh
./proton server&
uvx marimo run --sandbox github.py
```
