#  CreateStreamRequest serdes benchmark
```
commit 9baabe6a77931158cee4fc85367d4872e7ce682d

build: release with debuginfo

running: ./src/Cluster/examples/create_request_and_response_serde
```

## Benchmark Result

| Type     | Compressed | Write EPS | Read EPS |
| :------- | :--------: | :-------: | -------: |
| Native   |     No     |  7821032  |  5393855 |
| Protobuf |     No     |  3359851  |  2837222 |

#  FetchStreamRequest serdes benchmark

```
commit 9baabe6a77931158cee4fc85367d4872e7ce682d

build: release with debuginfo

running: ./src/Cluster/examples/fetch_request_and_response_serde
```

## Benchmark Result

| Type     | Compressed | Write EPS | Read EPS |
| :------- | :--------: | :-------: | -------: |
| Native   |     No     |  5952863  |  5480091 |
| Protobuf |     No     |  1582065  |   994237 |

#  ProduceStreamRequest serdes benchmark

```
commit 9baabe6a77931158cee4fc85367d4872e7ce682d

build: release with debuginfo

running: ./src/Cluster/examples/produce_request_and_response_serde
```

## result:

| Type     | Compressed | Write EPS | Read EPS |
| :------- | :--------: | :-------: | -------: |
| Native   |     No     | 9685313   |  7921253 |
| Protobuf |     No     |  3452311  |  1943573 |