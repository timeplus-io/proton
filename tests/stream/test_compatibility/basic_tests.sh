docker-compose -f test_compatibility/docker-compose.yaml up -d
sleep 5
docker exec proton-server proton client -nm -q "select x from example where _tp_time > earliest_ts() limit 3;"
docker exec proton-server proton client -nm -q "select x from example_external limit 3 settings seek_to='earliest';"
docker-compose -f test_compatibility/docker-compose.yaml down
