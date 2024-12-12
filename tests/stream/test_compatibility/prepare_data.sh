docker-compose -f test_compatibility/docker-compose.yaml up -d
sleep 5
docker exec proton-server proton client -nm -q "create stream example(x int);"
docker exec proton-server proton client -nm -q "insert into example (x) values (1)(2)(3);"
docker exec redpanda rpk topic create example_topic
docker exec proton-server proton client -nm -q "create external stream example_external (x int) settings type='kafka', brokers='stream-store:9092',topic='example_topic',data_format = 'JSONEachRow';"
docker exec proton-server proton client -nm -q "insert into example_external (x) values (2)(4)(6);"
docker-compose -f test_compatibility/docker-compose.yaml down
