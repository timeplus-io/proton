## distributed-dwal

`clickhouse-dwal-benchmark` is a tool which can be used to do basic performance teseting for KafkaWAL / KafkaWALSimpleConsumer / KafkaWALConsumer and also can be used to experiment the behavior of librdkafka / kafka like consumer group and offset commits.


## Offset commit experiments for KafkaWALConsumer

1. Create topic inc1, incr2 with 3 partitions for both

```
$ ./programs/clickhouse-dwal-benchmark topic --mode create --partition 3 --name inc1
$ ./programs/clickhouse-dwal-benchmark topic --mode create --partition 3 --name inc2
```

2. In one console, run incremental consumer with `STORED` offsets (-1000) for all partitions. Since there are no offsets stored in Kafka broker, the default `earliest` will be used.

```
./programs/clickhouse-dwal-benchmark consume --incremental 1 --group_id xxx --kafka_topic_partition_offsets inc1,0,-1000:inc1,1,-1000:inc1,2,-1000,inc2,0,-1000:inc2,1,-1000:inc2,2,-1000
```

3. In another console, run producer to ingest data to the topics above

```
$ ./programs/clickhouse-dwal-benchmark produce --kafka_topic inc1 --iterations 30 --batch_size 1
$ ./programs/clickhouse-dwal-benchmark produce --kafka_topic inc2 --iterations 30 --batch_size 1
```

4. Monitor the incremental consuming process in the console in step 2

5. Check the offsets committed to Kafka broker

```
$ ./bin/kafka-consumer-groups.sh  --describe --group xxx --bootstrap-server localhost:9092
```

6. Stop the consuming in step 2 and re-execute the same consume command. You will see there are no new message consumed in this step because there are no new messages ingest

7. Repeat step 3 to ingest more data and you shall see more data get consumed in the console in step 6

8. Repeat step 5 to check the offset committed to broker again. You will see new offsets get committed to broker

9. Stop the consuming in step 6 and execute the same consume command, but with different initial offsets this time. You will see this time, it will replay the message starting from offset 1 for all partitions.

```
./programs/clickhouse-dwal-benchmark consume --incremental 1 --group_id xxx --kafka_topic_partition_offsets inc1,0,1:inc1,1,1:inc1,2,1:inc2,0,1:inc2,1,1:inc2,2,1
```

10. Repeat step 3 to ingest more data and you shall see more data get consumed in the console in step 9

11. Repeat step 5 to check the offset committed to broker again. You will see new offsets get committed to broker

## Offset commit experiments for KafkaWALSimpleConsumer

1. Create topic simple, with 1 partition

```
$ ./programs/clickhouse-dwal-benchmark topic --mode create --partition 1 --name simple
```

2. In one console, run simple consumer with `STORED` offsets (-1000). Since there are no offsets stored in Kafka broker, the default `earliest` will be used.

```
./programs/clickhouse-dwal-benchmark consume --incremental 1 --group_id zzz --kafka_topic_partition_offsets simple,0,-1000
```

3. In another console, run producer to ingest data to the topics above

```
$ ./programs/clickhouse-dwal-benchmark produce --kafka_topic simple --iterations 30 --batch_size 1
```

4. Monitor the simple consuming process in the console in step 2

5. Check the offsets committed to Kafka broker

```
$ ./bin/kafka-consumer-groups.sh  --describe --group zzz --bootstrap-server localhost:9092
```

6. Stop the consuming in step 2 and re-execute the same consume command. You will see there are no new message consumed in this step because there are no new messages ingest

7. Repeat step 3 to ingest more data and you shall see more data get consumed in the console in step 6

8. Repeat step 5 to check the offset committed to broker again. You will see new offsets get committed to broker

9. Stop the consuming in step 6 and execute the same consume command, but with different initial offsets this time. It will NOT replay the message starting from offset 1 because internally KafkaWALSimpleConsumer first uses the STORED offset in broker to consume messages and the STORED offset already passes offset 1. Simple consumer can't replay old messages with the same group id.

```
./programs/clickhouse-dwal-benchmark consume --group_id zzz --kafka_topic_partition_offsets simple,0,1
```

10. Repeat step 3 to ingest more data and you shall see more data get consumed in the console in step 9

11. Repeat step 5 to check the offset committed to broker again. You will see there are new offsets get committed to broker.
