#!/bin/bash

NODES=${NODES:-3}
SUITE_NAME=${SUITE_NAME:-/}
LOG_DIR=${LOG_DIR:-"./log"}
FATAL_LOG_PATTERN='<Fatal>'

run_and_echo() {
  echo "+ $*"
  "$@"
}

function get_full_dir() {
    run_and_echo sudo mkdir -p ${LOG_DIR}
    FULL_LOG_DIR="$(cd "$(dirname "$LOG_DIR")" && pwd)/$(basename "$LOG_DIR")"
}

function prepare_log_p1() {
    echo "Creating and set permissions for log directory"
    run_and_echo sudo rm -rf ${LOG_DIR}/proton-server/log
    run_and_echo sudo mkdir -p ${LOG_DIR}/proton-server/log
    run_and_echo sudo chown -R 101:101 ${LOG_DIR}/proton-server*
    echo "Create log directory completed: $LOG_DIR"
}

function prepare_log_p3() {
    echo "Creating and set permissions for log directory"
    run_and_echo sudo rm -rf ${LOG_DIR}/{proton-server1,proton-server2,proton-server3}/log
    run_and_echo sudo mkdir -p ${LOG_DIR}/{proton-server1,proton-server2,proton-server3}/log
    run_and_echo sudo chown -R 101:101 ${LOG_DIR}/proton-server*
    echo "Create log directory completed: $LOG_DIR"
}

function prepare_log() {
    if [[ "$NODES" -eq 1 ]]; then
        prepare_log_p1
    elif [[ "$NODES" -eq 3 ]]; then
        prepare_log_p3
    else
        echo "Invalid NODES: $NODES, use node 3 by default"
        prepare_log_p3
    fi
}

function collect_fatal_log() {
    local FATAL_LOG_PATH=$1
    local FATAL_LOG_DIR=$(dirname $FATAL_LOG_PATH)

    if [[ "$FATAL_LOG_PATH" == *.log ]]; then
        FATAL_LOG_PATH+=".fatal"
    fi

    echo "Collecting fatal proton log to target path: ${FATAL_LOG_PATH}"
    run_and_echo sudo mkdir -p ${FATAL_LOG_DIR}

    for dir in ${LOG_DIR}/proton-server*/log/; do
        find "$dir" -type f -name "*.log" ! -name "*.err.log" | while read -r file; do
            echo "Checking file $file..."
            if sudo grep -Faq "${FATAL_LOG_PATTERN}" "$file"; then
                container=$(basename $(dirname $(dirname "$file")))
                echo "------------ [$SUITE_NAME] Logs from proton container ($file) -------------" | sudo tee -a "${FATAL_LOG_PATH}"
                sudo grep -Fa "${FATAL_LOG_PATTERN}" "$file" | sed "s|^|[$container] |" | sudo tee -a "${FATAL_LOG_PATH}"
                echo "----------------------------------------------------------------" | sudo tee -a "${FATAL_LOG_PATH}"
            fi
        done
    done
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

function collect_log() {
    local OUTPUT_DIR=$1
    OUTPUT_DIR="${OUTPUT_DIR%/}"

    collect_docker_log

    echo "Collecting proton log to target directory: ${OUTPUT_DIR}"
    run_and_echo sudo mkdir -p ${OUTPUT_DIR}

    for dir in ${LOG_DIR}/proton-server*/log/; do
        dir="${dir%/}"
        container=$(basename $(dirname "$dir"))
        echo "Checking container $container file ${dir}/proton-server.log..."

        if [ -f "${dir}/proton-server.log" ]; then
            run_and_echo sudo cp "${dir}/proton-server.log" "${OUTPUT_DIR}/${container}.log"
        fi

        if [ -f "${dir}/proton-server.err.log" ]; then
            run_and_echo sudo cp "${dir}/proton-server.err.log" "${OUTPUT_DIR}/${container}.err.log"
        fi
    done
}


# If the --log-dir argument is provided, it overrides the LOG_DIR environment variable.
args=("$@")
for ((i = 0; i < $#; i++)); do
  if [[ "${args[i]}" == "--log-dir" ]]; then
    LOG_DIR="${args[i+1]}"
    echo "LOG_DIR set to: $LOG_DIR"
    break
  fi
done

while [[ $1 == --* ]]
do
    if [[ $1 == '--init' ]]; then
        prepare_log
        shift
    elif [[ $1 == '--log-dir' ]]; then
        echo "LOG_DIR already set, skipping --log-dir"
        shift 2
    elif [[ $1 == '--output-dir' ]]; then
        collect_log $2
        shift 2
    elif [[ $1 == '--fatal-log' ]]; then
        collect_fatal_log $2
        shift 2
    else
        echo "Unknown option $1"
        echo "Usage: $0 [--init | --log-dir | --output-dir | --fatal-log]"
        exit 2
    fi
done
