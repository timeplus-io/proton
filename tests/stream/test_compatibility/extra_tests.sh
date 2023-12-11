docker-compose -f test_compatibility/docker-compose.yaml up -d
docker exec -it proton-server proton client -nm -q "insert into example (x) values (4)(5)(6);"
docker exec -it proton-server proton client -nm -q "select x from example where _tp_time > earliest_ts() limit 6;"
docker exec -it proton-server proton client -nm -q "insert into example_external (x) values (8)(10)(12);"
docker exec -it proton-server proton client -nm -q "select x from example_external limit 6 settings seek_to='earliest';"
docker-compose -f test_compatibility/docker-compose.yaml down
