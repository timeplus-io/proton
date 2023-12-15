set -e
CUR_DIR="$GITHUB_WORKSPACE/tests/stream/test_compatibility"
docker-compose -f "$CUR_DIR/configs/docker-compose.yaml" up -d
docker ps
sleep 5
export TEST_DIR="$CUR_DIR/extra_tests"
python $CUR_DIR/run_compatibility_tests.py
docker exec proton-server pkill proton-server
sleep 10
docker-compose -f "$CUR_DIR/configs/docker-compose.yaml" down -v
