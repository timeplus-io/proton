set -e

LOCAL_WORKSPACE="../../../../proton"

export GITHUB_WORKSPACE=${GITHUB_WORKSPACE:-$LOCAL_WORKSPACE}
export TEST_DIR=${GITHUB_WORKSPACE}/tests/cluster

CUR_DIR=${GITHUB_WORKSPACE}/tests/stream/test_compatibility
CLUSTER_FOLDER_DIR=${GITHUB_WORKSPACE}/tests/cluster
LOG_DIR=${GITHUB_WORKSPACE}/log
NODES=${NODES:-3}

OS=$(uname)
ARCH=$(uname -m)

if [ "$CI" == "true" ]; then
    export SSH_USER=ubuntu
    export SSH_DIR=/home/ubuntu/.ssh
fi

function prepare_pulsar_folder() {
    # change folder permission for pulser data
    sudo mkdir -p ${GITHUB_WORKSPACE}/data/p3k1_pulsar ${GITHUB_WORKSPACE}/data/p1k1_pulsar
    sudo chmod 775 ${GITHUB_WORKSPACE}/data/p3k1_pulsar ${GITHUB_WORKSPACE}/data/p1k1_pulsar
}

function prepare_test_input() {
    # copy compatibility test folder under data directory and upload
    # to align test cases scope between compared versions
    mkdir -p ${GITHUB_WORKSPACE}/data/deployment/
    cp -r ${GITHUB_WORKSPACE}/tests/cluster/compatibility/ ${GITHUB_WORKSPACE}/data/
    cp -r ${GITHUB_WORKSPACE}/tests/cluster/deployment/compatibility/* ${GITHUB_WORKSPACE}/data/deployment/

    # set proton volume as user 101
    mkdir -p ${GITHUB_WORKSPACE}/data/{proton-server,proton-server1,proton-server2,proton-server3}/{datas,log}
    sudo chown -R 101:101 ${GITHUB_WORKSPACE}/data/proton-server*

    # download data
    if [ "$CI" == "true" ]; then
        aws s3 cp --no-progress --recursive s3://tp-internal2/proton-oss/compatibility/enterprise/data ${GITHUB_WORKSPACE}/data/dataset
    fi
}

function deploy_up() {
    prepare_pulsar_folder
    bash ${CLUSTER_FOLDER_DIR}/ssh.sh init
    docker-compose -p compa_p1k1 -f ${CUR_DIR}/docker-compose-p1k1.yaml up -d
    if [[ "$NODES" -eq 3 ]]; then
        docker-compose -p compa_p3k1 -f ${CUR_DIR}/docker-compose-p3k1.yaml up -d
    fi
    sleep 60
    docker ps
}

function deploy_down() {
    docker ps
    docker-compose -p compa_p1k1 -f ${CUR_DIR}/docker-compose-p1k1.yaml down
    if [[ "$NODES" -eq 3 ]]; then
        docker-compose -p compa_p3k1 -f ${CUR_DIR}/docker-compose-p3k1.yaml down
    fi
    sleep 5
    bash ${CLUSTER_FOLDER_DIR}/ssh.sh clean
}

function prepare_data() {
    prepare_test_input
    deploy_up
    docker exec p1k1_probe probe smoke -v /compatibility -d /deploy -c p1k1 -t prepare --os ${OS} --arch ${ARCH} -f /log/probe.log &
    if [[ "$NODES" -eq 3 ]]; then
        docker exec p3k1_probe probe smoke -v /compatibility -d /deploy -c p3k1 -t prepare --os ${OS} --arch ${ARCH} -f /log/probe.log &
    fi
    wait
    deploy_down
    summary_log
}

function validate_test() {
    deploy_up
    docker exec p1k1_probe probe smoke -v /compatibility -d /deploy -c p1k1 -t validate --os ${OS} --arch ${ARCH} -f /log/probe.log &
    if [[ "$NODES" -eq 3 ]]; then
        docker exec p3k1_probe probe smoke -v /compatibility -d /deploy -c p3k1 -t validate --os ${OS} --arch ${ARCH} -f /log/probe.log &
    fi
    wait
    deploy_down
    summary_log
}

function collect_log() {
    local cluster="$1"
    echo "[$cluster] collect_log begin"

    mkdir -p ${LOG_DIR}/${cluster}
    grep -r "Gatherer" ${LOG_DIR}/${cluster}/probe.log >> ${LOG_DIR}/summary.log || true

    echo "[$cluster] Probe log:"
    cat ${LOG_DIR}/${cluster}/probe.log
    echo "Summary log:"
    cat ${LOG_DIR}/summary.log

    if [ ! -f ${LOG_DIR}/${cluster}/.status ]; then
        echo "Test failed with panic" >> ${LOG_DIR}/.fail
    elif grep -q "succeed" ${LOG_DIR}/${cluster}/.status; then
        echo "Test succeed"
    elif [ ! -f "${LOG_DIR}/${cluster}/.wrong" ]; then
        echo "Test failed with no wrong cases found, it's caused by unrelated setup or teardown"
    else
        line=$(head -n 1 ${LOG_DIR}/${cluster}/.wrong)
        echo "Failed cases: $line" >> ${LOG_DIR}/.fail
    fi
    echo "[$cluster] collect_log end"
}

function collect_proton_log() {
    bash ${GITHUB_WORKSPACE}/tests/cluster/log.sh --log-dir ${GITHUB_WORKSPACE}/data --fatal-log ${LOG_DIR}/.fail
    bash ${GITHUB_WORKSPACE}/tests/cluster/log.sh --log-dir ${GITHUB_WORKSPACE}/data --fatal-log /artifacts/log/proton.log.fatal
    bash ${GITHUB_WORKSPACE}/tests/cluster/log.sh --log-dir ${GITHUB_WORKSPACE}/data --output-dir /artifacts/log
}

function summary_log() {
    collect_log "p1k1"
    collect_log "p3k1"
    collect_proton_log

    echo "====================Compatibility Testing Result Summary===================="
    if [ -f ${LOG_DIR}/.fail ]; then
        echo "Test failed with cases execution" | tee -a $GITHUB_STEP_SUMMARY

        fail_output=$(cat ${LOG_DIR}/.fail)
        if [ ${#fail_output} -gt 900 ]; then
            cat ${LOG_DIR}/.fail
            cat ${LOG_DIR}/.fail | head -c 900 >> $GITHUB_STEP_SUMMARY
            echo "-" >> $GITHUB_STEP_SUMMARY
            echo "### Output is too large, truncated to 900 bytes." >> $GITHUB_STEP_SUMMARY
        else
            echo "$fail_output" | tee -a $GITHUB_STEP_SUMMARY
        fi

        sudo rm -f ${LOG_DIR}/.fail
        echo "====================Compatibility Testing Result Summary===================="
        exit 1
    else
        echo "All tests succeeded" | tee -a $GITHUB_STEP_SUMMARY
        echo "====================Compatibility Testing Result Summary===================="
    fi
}


if [ "$1" == "prepare" ]; then
    prepare_data
    cd ${GITHUB_WORKSPACE}
    tar -zcvf ${PROTON_VERSION}.tar.gz data
    cd ${GITHUB_WORKSPACE}/tests/stream
elif [ "$1" == "validate" ]; then
    validate_test
else
    echo "please specify operation: prepare or validate."
    exit 1
fi
