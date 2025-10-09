set -e

NODES=${NODES:-3}
PERFORMANCE_RESOURCE_FOLDER="./resource"
export DATA_DIR="./resource/data"
export RESULT_DIR="./resource/result"
BENCHMARK_DIR="./resource/benchmark"
SUMMARY_DIR="./resource/summary"
PROTON_COMPARE_VERSION="2.0.53,2.3.16,2.3.30"
exit_code=0

export CONTAINER_NAME_PREFIX=1


function performance_test_p1k1() {
    export CLUSTER=p1k1
    bash ./log.sh --init --log-dir ./log

    docker-compose -p perf_p1k1 -f ./deployment/docker-compose-p1k1.yaml up -d
    sleep 10
    docker ps -a
    collect_docker_log

    docker exec ${CONTAINER_NAME_PREFIX}_p1k1_probe probe smoke -v /perf -d /deploy/performance -c p1k1 -b nexmark -f /log/probe.log
    collect_log "p1k1"

    docker-compose -p perf_p1k1 -f ./deployment/docker-compose-p1k1.yaml down -v
    sleep 5
}

function performance_test_p3k1() {
    export CLUSTER=p3k1
    bash ./log.sh --init --log-dir ./log

    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml up -d
    sleep 10
    docker ps -a
    collect_docker_log

    # docker exec -i ${CONTAINER_NAME_PREFIX}_p3k1_stream-store bash -c "
    #     cat /home/data/nexmark_auction.json | /opt/redpanda/bin/rpk topic produce nexmark-auction --brokers=stream-store:29092 &&
    #     cat /home/data/nexmark_bid.json | /opt/redpanda/bin/rpk topic produce nexmark-bid --brokers=stream-store:29092 &&
    #     cat /home/data/nexmark_person.json | /opt/redpanda/bin/rpk topic produce nexmark-person --brokers=stream-store:29092
    # "

    docker exec ${CONTAINER_NAME_PREFIX}_p3k1_probe probe smoke -v /perf -d /deploy/performance -c p3k1 -b nexmark -f /log/probe.log
    collect_log "p3k1"

    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml down -v
    sleep 5
}

function collect_log() {
    local cluster="$1"

    LOG_DIR=./log/${cluster}/${CONTAINER_NAME_PREFIX}_log
    mkdir -p ${LOG_DIR}
    sudo chmod u+w ./log
    sudo chown $(whoami):$(whoami) ./log
    sudo grep -r "Gatherer" ${LOG_DIR}/probe.log >> ./log/summary.log

    echo "[$cluster] Probe log:"
    sudo cat ${LOG_DIR}/probe.log
    echo "Summary log:"
    sudo cat ./log/summary.log

    if [ ! -f ${LOG_DIR}/.status ]; then
        sudo echo "Test failed with panic" >> ./log/.fail
    elif grep -q "succeed" ${LOG_DIR}/.status; then
        echo "Test succeed"
    elif [ ! -f "${LOG_DIR}/.wrong" ]; then
        echo "Test failed with no wrong cases found, it's caused by unrelated setup or teardown"
    else
        line=$(head -n 1 ${LOG_DIR}/.wrong)
        sudo echo "Failed cases: $line" >> ./log/.fail
    fi
}

function collect_docker_log() {
    for container in $(docker ps -a --format '{{.Names}}' | grep timeplus); do
        status=$(docker inspect -f '{{.State.Status}}' "$container")
        if [ "$status" = "restarting" ] || [ "$status" = "exited" ]; then
            echo "Container '$container' is $status. Logs:"
            docker logs "$container"
        fi
    done
}

function summary_log() {
    echo "====================Performance Testing Result Summary===================="
    if [ -f ./log/.fail ]; then
        echo "Some tests failed"
        sudo cat ./log/.fail
        sudo rm -f ./log/.fail
        echo "====================Performance Testing Result========================"
        exit 1
    else
        echo "All tests succeeded"
        echo "====================Performance Testing Result========================"
    fi
}

function generate_github_summary() {
    if [ "$CI" == "true" ]; then
        md_file="$SUMMARY_DIR/table.md"
        if [ -e "$md_file" ]; then
            echo "Performance Testing Benchmark Summary" >> $GITHUB_STEP_SUMMARY
            echo "" >> $GITHUB_STEP_SUMMARY
            while IFS= read -r line; do
                echo "$line" >> $GITHUB_STEP_SUMMARY
            done < "$md_file"
            rm $md_file
        fi
    fi
}

function download_data_benchmark_from_s3() {
    echo "Download data and benchmark from S3"

    if [ "$CI" == "true" ]; then
        sudo rm -rf ./log $DATA_DIR $BENCHMARK_DIR $RESULT_DIR $SUMMARY_DIR
        mkdir -p $DATA_DIR $BENCHMARK_DIR $RESULT_DIR $SUMMARY_DIR
        aws s3 cp --no-progress --recursive s3://tp-internal2/proton-oss/cluster/enterprise/performance/data $DATA_DIR
        aws s3 cp --no-progress --recursive s3://tp-internal2/proton-oss/cluster/enterprise/performance/benchmark $BENCHMARK_DIR
    fi

    if [ -d "$DATA_DIR" ] && [ -z "$(find "$DATA_DIR" -mindepth 1 -print -quit)" ]; then
        echo "failed to download data from s3, exit"
        exit 1
    fi
    if [ -d "$BENCHMARK_DIR" ] && [ -z "$(find "$BENCHMARK_DIR" -mindepth 1 -print -quit)" ]; then
        echo "failed to download data from s3, exit"
        exit 1
    fi
}

function run_performance_test() {
    mkdir -p ./log
    export FULL_LOG_DIR="$(cd "$(dirname "./log")" && pwd)/$(basename "./log")"
    echo "FULL_LOG_DIR set to $FULL_LOG_DIR"

    bash ./ssh.sh init
    performance_test_p1k1
    if [[ "$NODES" -eq 3 ]]; then
        performance_test_p3k1
    fi
    bash ./ssh.sh clean
    summary_log
}

function compare_performance_result() {
    if [ "$CI" == "true" ]; then
        # make virtualenv
        cd ${GITHUB_WORKSPACE}/tests/proton_ci
        ln -s /usr/bin/python3 /usr/bin/python
        apt-get update
        systemctl stop unattended-upgrades
        apt install python3-venv jq -y
        python -m venv env
        source env/bin/activate
        pip install -r requirements.txt
        cd ${GITHUB_WORKSPACE}/tests/cluster/
    fi

    set +e
    python ../proton_ci/performance_compare.py --versions $PROTON_COMPARE_VERSION --dir $PERFORMANCE_RESOURCE_FOLDER --threshold 50% --save-benchmark
    exit_code=$?
    echo "exit code inside function: $exit_code"
    set -e
}

function upload_result_to_s3() {
    if [ "$CI" == "true" ]; then
        echo "Upload result to S3"
        aws s3 cp --no-progress --recursive ./log s3://tp-internal2/proton-oss/cluster/enterprise/performance/log
        aws s3 cp --no-progress --recursive $RESULT_DIR s3://tp-internal2/proton-oss/cluster/enterprise/performance/result
        if [ -d "$BENCHMARK_DIR/new" ]; then
            aws s3 cp --no-progress --recursive ${BENCHMARK_DIR}/new s3://tp-internal2/proton-oss/cluster/enterprise/performance/benchmark
        fi
        if [ "$(ls -A $SUMMARY_DIR)" ]; then
            cp -r $SUMMARY_DIR/* /artifacts # upload summary result to artifacts in Github
            aws s3 cp --no-progress --recursive $SUMMARY_DIR s3://tp-internal2/proton-oss/cluster/enterprise/performance/summary
        else
            echo "No summary found, skip upload"
        fi
    fi
}

function print_summary() {
    generate_github_summary

    if [ $exit_code -ne 0 ]; then
        echo "====================Performance Testing Benchmark Summary===================="
        echo "Benchmark test failed, performance warning found!"
        echo "====================Performance Testing Benchmark Summary===================="
        exit $exit_code
    fi

    echo "====================Performance Testing Benchmark Summary===================="
    echo "Benchmark test pass, no performance warning found."
    echo "====================Performance Testing Benchmark Summary===================="
}


if [[ -z "$PROTON_VERSION" ]]; then
    echo "PROTON_VERSION environment variable is not set or is empty. Exiting..."
    exit 1
fi

if [[ -z "$GH_PERSONAL_ACCESS_TOKEN" ]]; then
    echo "GH_PERSONAL_ACCESS_TOKEN environment variable is not set or is empty. Exiting..."
    exit 1
fi

if [ "$CI" == "true" ]; then
    export SSH_USER=ubuntu
    export SSH_DIR=/home/ubuntu/.ssh
fi

download_data_benchmark_from_s3
run_performance_test
compare_performance_result
upload_result_to_s3
print_summary
