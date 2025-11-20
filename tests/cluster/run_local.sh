#!/bin/bash

# Usage: sudo -E bash run_local.sh
#
# Required Environment Variables:
# - PROTON_VERSION  Timeplusd version
# - SUITE           Test suite name
# - SSH_USER        Local ssh user
# 
# Optional Environment Variables:
# - NODES           Deploy nodes number (support: 1, 3)
# - HYBRID          Enable hybrid mode (default: '')
#
# Example::
# export PROTON_VERSION=3.0.1-rc.10
# export SUITE=dedup
# export SSH_USER=$(whoami)
# export NODES=1
# export HYBRID=true
# sudo -E bash run_local.sh


PROTON_VERSION=${PROTON_VERSION}
PROTON_REPO=${PROTON_REPO:-"timeplus/timeplusd"}
NODES=${NODES:-3}
CLUSTER="p${NODES}k1"
SUITE=${SUITE}
LOG_DIR=./log
SUB_LOG_DIR=${LOG_DIR}/${CLUSTER}/1_log
HYBRID=${HYBRID:-''}

OS=$(uname)
ARCH=$(uname -m)

export SSH_DIR=$HOME/.ssh
export CONTAINER_NAME_PREFIX=1
export FULL_LOG_DIR="$(cd "$(dirname "$LOG_DIR")" && pwd)/$(basename "$LOG_DIR")"


run_and_echo() {
  echo "+ $*"
  "$@"
}

function build_smoke_image() {
    DOCKER_DIR=../../docker/test/smoke

    run_and_echo docker build $DOCKER_DIR -f $DOCKER_DIR/Dockerfile --build-arg FROM_TAG=$PROTON_VERSION --build-arg FROM_REPO=$PROTON_REPO -t timeplus/proton-smoke-test:$PROTON_VERSION
}

function set_deploy_cmd() {
    CLUSTER="p${NODES}k1"
    PROFFIEL_FLAG=""
    case "$SUITE" in
        dictionary)
            PROFFIEL_FLAG="--profile source"
            ;;
        http_external_stream)
            PROFFIEL_FLAG="--profile splunk"
            ;;
        external_table_mysql)
            PROFFIEL_FLAG="--profile mysql"
            ;;
        external_table_clickhouse)
            PROFFIEL_FLAG="--profile clickhouse"
            ;;
        external_table_postgres)
            PROFFIEL_FLAG="--profile postgres"
            ;;
        external_table_s3|storage)
            PROFFIEL_FLAG="--profile minio"
            CLUSTER="p${NODES}m1"
            ;;
        *)
            echo "test with default deploy"
            ;;
    esac

    if [[ -n "$HYBRID" ]] && [[ "$NODES" -eq 3 ]];then
        echo "Smoke test executed in hybrid mode"
        AGGREGATION_CHECK_MODE=random
        OVERRIDE_FLAG="-f ./deployment/docker-compose-p${NODES}k1.override.yaml"
        BLACKLIST_FLAG="-b not_supported_hybrid"
    fi
}

function set_suite_filter() {
    if [ -n "$SUITE" ]; then
        FILTER="-s $SUITE"
        echo "FILTER set to '$FILTER'"
    else
        echo "not set test suite filter"
    fi
}

function smoke_test() {
    # Ensure any stale project/volumes from previous aborted runs are removed
    run_and_echo docker compose -p smoke_${CLUSTER} -f ./deployment/docker-compose-${CLUSTER}.yaml $PROFFIEL_FLAG $OVERRIDE_FLAG down -v || true
    run_and_echo docker compose -p smoke_${CLUSTER} -f ./deployment/docker-compose-${CLUSTER}.yaml $PROFFIEL_FLAG $OVERRIDE_FLAG up -d
    run_and_echo sleep 15
    run_and_echo docker ps -a

    run_and_echo docker exec 1_${CLUSTER}_probe probe smoke -v /smoke -d /deploy/smoke -c ${CLUSTER} --os ${OS} --arch ${ARCH} ${FILTER} ${BLACKLIST_FLAG} --variable CONTAINER_NAME_PREFIX=${CONTAINER_NAME_PREFIX} --variable sanitizer=${SANITIZER} --variable nodes=${NODES} -f /log/probe.log

    run_and_echo docker compose -p smoke_${CLUSTER} -f ./deployment/docker-compose-${CLUSTER}.yaml $PROFFIEL_FLAG $OVERRIDE_FLAG down -v
    run_and_echo sleep 10
    run_and_echo docker ps -a
}

function collect_probe_log() {
    run_and_echo mkdir -p ${SUB_LOG_DIR}

    run_and_echo sudo grep -r "Gatherer" ${SUB_LOG_DIR}/probe.log | sudo tee -a ${LOG_DIR}/summary.log

    echo "[$CLUSTER] Probe log:"
    run_and_echo sudo cat ${SUB_LOG_DIR}/probe.log
    echo "Summary log:"
    run_and_echo sudo cat ${LOG_DIR}/summary.log

    if [ ! -f ${SUB_LOG_DIR}/.status ]; then
        sudo echo "Test failed with panic" | sudo tee -a ${LOG_DIR}/.fail
    elif grep -q "succeed" ${SUB_LOG_DIR}/.status; then
        echo "Test succeed"
    elif [ ! -f "${SUB_LOG_DIR}/.wrong" ]; then
        echo "Test failed with no wrong cases found, it's caused by unrelated setup or teardown"
    else
        line=$(head -n 1 ${SUB_LOG_DIR}/.wrong)
        sudo echo "Failed cases: $line" | sudo tee -a ${LOG_DIR}/.fail
    fi
}

function collect_timeplusd_log() {
    bash log.sh --log-dir ${LOG_DIR} --fatal-log ${LOG_DIR}/.fail
}

function summary_log() {
    collect_probe_log
    collect_timeplusd_log

    echo "====================Migration Testing Result===================="
    if [ -f ${LOG_DIR}/.fail ]; then
        echo "Test failed with cases execution"
        
        fail_output=$(cat ${LOG_DIR}/.fail)
        if [ ${#fail_output} -gt 900 ]; then
            cat ${LOG_DIR}/.fail
            cat ${LOG_DIR}/.fail | head -c 900
            echo "-"
            echo "### Output is too large, truncated to 900 bytes."
        else
            echo "$fail_output"
        fi

        sudo rm -f ${LOG_DIR}/.fail
        echo "====================Migration Testing Result========================"
        exit 1
    else
        echo "All tests succeeded"
        echo "====================Migration Testing Result========================"
    fi
}

function run_smoke_test() {
    sudo rm -rf ${LOG_DIR}
    bash ./ssh.sh init
    bash ./log.sh --init --log-dir ${LOG_DIR}
    set_deploy_cmd
    set_suite_filter
    smoke_test
    bash ./ssh.sh clean
    summary_log
}

if [[ -z "$PROTON_VERSION" ]]; then
    echo "PROTON_VERSION environment variable is not set or is empty. Exiting..."
    exit 1
fi

if [[ -z "$SUITE" ]]; then
    echo "SUITE environment variable is not set or is empty. Exiting..."
    exit 1
fi

build_smoke_image
run_smoke_test
