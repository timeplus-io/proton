set -e


export SSH_USER=ubuntu
export SSH_DIR=/home/ubuntu/.ssh
export CI=${CI:-True}
export SMOKE_FOLDER="./smoke"
export END2END_FOLDER="../end2end"
export PERFORMANCE_FOLDER="./performance"
export PERFORMANCE_RESOURCE_FOLDER="./resource"
export DATA_DIR="${PERFORMANCE_RESOURCE_FOLDER}/data"
export CONTAINER_NAME_PREFIX=1
export SOURCE_DATA_DIR="${PERFORMANCE_RESOURCE_FOLDER}/source_data"
export TARGET_DATA_DIR="${PERFORMANCE_RESOURCE_FOLDER}/target_data"
export SOURCE_RESULT_DIR="${PERFORMANCE_RESOURCE_FOLDER}/source_result"
export TARGET_RESULT_DIR="${PERFORMANCE_RESOURCE_FOLDER}/target_result"


SUMMARY_DIR=${PERFORMANCE_RESOURCE_FOLDER}/summary

function performance_test_source() {
    echo "Starting performance test with source image: ${SOURCE_VERSION}"
    export PROTON_VERSION=$SOURCE_VERSION
    export RESULT_DIR=$SOURCE_RESULT_DIR
    export DATA_DIR=$SOURCE_DATA_DIR
    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml up -d
    sleep 8   
    docker ps

    docker exec -i ${CONTAINER_NAME_PREFIX}_p3k1_stream-store bash -c "
        cat /home/data/nexmark_auction.json | /opt/redpanda/bin/rpk topic produce nexmark-auction --brokers=stream-store:29092 &&
        cat /home/data/nexmark_bid.json | /opt/redpanda/bin/rpk topic produce nexmark-bid --brokers=stream-store:29092 &&
        cat /home/data/nexmark_person.json | /opt/redpanda/bin/rpk topic produce nexmark-person --brokers=stream-store:29092
    "

    docker exec ${CONTAINER_NAME_PREFIX}_p3k1_probe probe smoke -v /perf -d /deploy/performance -c p3k1 -f /log/probe.log
    collect_log "p3k1"

    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml down -v
    sleep 5
}

function performance_test_target() {
    echo "Starting performance test with target image: ${TARGET_VERSION}"
    export PROTON_VERSION=$TARGET_VERSION
    export RESULT_DIR=$TARGET_RESULT_DIR
    export DATA_DIR=$TARGET_DATA_DIR
    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml up -d
    sleep 8
    docker ps

    docker exec -i ${CONTAINER_NAME_PREFIX}_p3k1_stream-store bash -c "
        cat /home/data/nexmark_auction.json | /opt/redpanda/bin/rpk topic produce nexmark-auction --brokers=stream-store:29092 &&
        cat /home/data/nexmark_bid.json | /opt/redpanda/bin/rpk topic produce nexmark-bid --brokers=stream-store:29092 &&
        cat /home/data/nexmark_person.json | /opt/redpanda/bin/rpk topic produce nexmark-person --brokers=stream-store:29092
    "

    docker exec ${CONTAINER_NAME_PREFIX}_p3k1_probe probe smoke -v /perf -d /deploy/performance -c p3k1 -f /log/probe.log
    collect_log "p3k1"

    docker-compose -p perf_p3k1 -f ./deployment/docker-compose-p3k1-perf.yaml down -v
    sleep 5
}

function collect_log() {
    local cluster="$1"

    LOG_DIR=./log/${cluster}/${CONTAINER_NAME_PREFIX}_log
    mkdir -p ${LOG_DIR}
    grep -r "Gatherer" ${LOG_DIR}/probe.log >> ./log/summary.log

    echo "[$cluster] Probe log:"
    cat ${LOG_DIR}/probe.log
    echo "Summary log:"
    cat ./log/summary.log

    if [ ! -f ${LOG_DIR}/.status ]; then
        echo "Test failed with panic" >> ./log/.fail
    elif grep -q "succeed" ${LOG_DIR}/.status; then
        echo "Test succeed"
    elif [ ! -f "${LOG_DIR}/.wrong" ]; then
        echo "Test failed with no wrong cases found, it's caused by unrelated setup or teardown"
    else
        line=$(head -n 1 ${LOG_DIR}/.wrong)
        echo "Failed cases: $line" >> ./log/.fail
    fi
}

function download_data_benchmark_from_s3() {
    echo "Download data and benchmark from S3"

    if [ "$CI" == "true" ]; then
        sudo rm -rf ./log $DATA_DIR $SOURCE_DATA_DIR $TARGET_DATA_DIR $SOURCE_RESULT_DIR $TARGET_RESULT_DIR $SUMMARY_DIR
        mkdir -p $DATA_DIR $SOURCE_DATA_DIR $TARGET_DATA_DIR $SOURCE_RESULT_DIR $TARGET_RESULT_DIR $SUMMARY_DIR
        aws s3 cp --no-progress --recursive s3://timeplus-ci-internal/proton-oss/cluster/enterprise/performance/data $DATA_DIR
    fi

    if [ -d "$DATA_DIR" ] && [ -z "$(find "$DATA_DIR" -mindepth 1 -print -quit)" ]; then
        echo "failed to download data from s3, exit"
        exit 1
    fi
}

function performance_prepare() {
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
}

function summary_log() {
    echo "====================Performance Testing Result Summary===================="
    if [ -f ./log/.fail ]; then
        echo "Some tests failed"
        cat ./log/.fail
        rm -f ./log/.fail
        echo "====================Performance Testing Result========================"
        exit 1
    else
        echo "All tests succeeded"
        echo "====================Performance Testing Result========================"
    fi
}

function generate_github_summary() {
    md_file="$SUMMARY_DIR/table.md"
    if [ -e "$md_file" ]; then
        echo "Performance Testing Benchmark Summary" >> $GITHUB_STEP_SUMMARY
        echo "" >> $GITHUB_STEP_SUMMARY
        while IFS= read -r line; do
            echo "$line" >> $GITHUB_STEP_SUMMARY
        done < "$md_file"
        rm $md_file
    fi
}

echo "SSH_DIR:${SSH_DIR}"
echo "SSH_USER:${SSH_USER}"

download_data_benchmark_from_s3

bash ./ssh.sh init
performance_test_source
performance_test_target
bash ./ssh.sh clean

summary_log

performance_prepare
set +e

python3 ../proton_ci/run_performance_comparison.py --dir $PERFORMANCE_RESOURCE_FOLDER --threshold 120%
exit_code=$?

cp -r $SUMMARY_DIR/* /artifacts # upload summary result to artifacts in Github
aws s3 cp --no-progress --recursive $SUMMARY_DIR s3://timeplus-ci-internal/proton-oss/cluster/enterprise/performance/summary
aws s3 cp --no-progress --recursive $SOURCE_RESULT_DIR s3://timeplus-ci-internal/proton-oss/cluster/enterprise/performance/source_result
aws s3 cp --no-progress --recursive $TARGET_RESULT_DIR s3://timeplus-ci-internal/proton-oss/cluster/enterprise/performance/target_result
aws s3 cp --no-progress --recursive ./log s3://timeplus-ci-internal/proton-oss/cluster/enterprise/performance/log

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
