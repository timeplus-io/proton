set -e

NODES=${NODES:-3}
CLUSTER="p${NODES}s1"
SUITE=${SUITE}
LOG_DIR=./log
SUB_LOG_DIR=${LOG_DIR}/${CLUSTER}/log
export FULL_LOG_DIR="$(cd "$(dirname "$LOG_DIR")" && pwd)/$(basename "$LOG_DIR")"


run_and_echo() {
  echo "+ $*"
  "$@"
}

function set_suite_filter() {
    if [ -n "$SUITE" ]; then
        FILTER="-s $SUITE"
        echo "FILTER set to '$FILTER'"
    else
        echo "not set test suite filter"
    fi
}

function migration_test() {
    run_and_echo docker-compose -p migration_${CLUSTER} -f ./deployment/docker-compose-${CLUSTER}-migration.yaml up -d
    run_and_echo sleep 10
    run_and_echo docker ps -a

    run_and_echo docker exec ${CLUSTER}_probe probe smoke -v /migration -d /deploy/migration -c ${CLUSTER} ${FILTER} -f /log/probe.log

    run_and_echo docker-compose -p migration_${CLUSTER} -f ./deployment/docker-compose-${CLUSTER}-migration.yaml down -v
    run_and_echo sleep 5
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

function collect_proton_log() {
    bash log.sh --log-dir ${LOG_DIR} --fatal-log ${LOG_DIR}/.fail
    bash log.sh --log-dir ${LOG_DIR} --fatal-log /artifacts/log/proton.log.fatal
    bash log.sh --log-dir ${LOG_DIR} --output-dir /artifacts/log
}

function summary_log() {
    collect_probe_log
    collect_proton_log

    echo "====================Migration Testing Result===================="
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
        echo "====================Migration Testing Result========================"
        exit 1
    else
        echo "All tests succeeded"
        echo "====================Migration Testing Result========================"
    fi
}

function run_migration_test() {
    bash ./ssh.sh init
    bash ./log.sh --init --log-dir ${LOG_DIR}
    set_suite_filter
    migration_test
    bash ./ssh.sh clean
    summary_log
}

if [[ -z "$PROTON_VERSION" ]]; then
    echo "PROTON_VERSION environment variable is not set or is empty. Exiting..."
    exit 1
fi

if [ "$CI" == "true" ]; then
    export SSH_USER=ubuntu
    export SSH_DIR=/home/ubuntu/.ssh
fi

run_migration_test
