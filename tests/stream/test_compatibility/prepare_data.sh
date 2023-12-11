docker-compose -f test_compatibility/docker-compose.yaml up -d
docker exec -it proton-server proton client -nm -q "create stream example(x int);"
docker exec -it proton-server proton client -nm -q "insert into example (x) values (1)(2)(3);"
docker exec -it redpanda rpk topic create example_topic
docker exec -it proton-server proton client -nm -q "create external stream example_external (x int) settings type='kafka', brokers='stream-store:9092',topic='example_topic',data_format = 'JSONEachRow';"
docker exec -it proton-server proton client -nm -q "insert into example_external (x) values (2)(4)(6);"
docker-compose -f test_compatibility/docker-compose.yaml down
