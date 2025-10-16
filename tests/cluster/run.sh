set -e

sudo rm -rf ./log
rm -rf ./.fail
mkdir -p /artifacts/log

# Capture baseline FD count for leak detection
echo "=== FD Baseline Check ==="
mkdir -p /artifacts/fd_diagnostics
BASELINE_FD_COUNT=$(ls -l /proc/*/fd/* 2>/dev/null | grep deleted | wc -l)
echo "Baseline deleted FD count: $BASELINE_FD_COUNT" > /artifacts/fd_diagnostics/fd_baseline.txt
ls -l /proc/*/fd/* 2>/dev/null | grep deleted > /artifacts/fd_diagnostics/fd_baseline_dump.txt
echo "Test run baseline - Deleted FD count: $BASELINE_FD_COUNT"

MAX_RETRIES=3
TOTAL_NAMES=""
SUITE_TIMEOUT=20m
BLACKLIST=("python_udf_library")
NODES=${NODES:-1}

OS=$(uname)
ARCH=$(uname -m)

# Set args for run.sh script
input="$1"
if [[ -z "$input" ]]; then
    echo "Smoke test executed by default"
fi
echo "$input"
IFS=";" read -ra features <<< "$input"

run_and_echo() {
  echo "+ $*"
  "$@"
}

contains() {
    local e match="$1"
    shift
    for e; do [[ "$e" == "$match" ]] && return 0; done
    return 1
}

add_to_blacklist() {
    if ! contains "$1" "${BLACKLIST[@]}"; then
        BLACKLIST+=("$1")
    fi
}

remove_from_blacklist() {
    local new_blacklist=()
    for item in "${BLACKLIST[@]}"; do
        if [[ "$item" != "$1" ]]; then
            new_blacklist+=("$item")
        fi
    done
    BLACKLIST=("${new_blacklist[@]}")
}

function test_hybrid_config() {
    local AGGREGATION_CHECK_MODE=$1
    HYBRID_FIELDS=('default_hash_table: hybrid' 'default_hash_join: hybrid' 'max_hot_keys: 2')
    MISSING_FIELD=""

    if [ "$AGGREGATION_CHECK_MODE" == "random" ]; then
        export CONTAINER_NAME_PREFIX=0
        export DOCKER_PROJECT="smoke_0"
        export LOG_DIR="./log/p${NODES}k1/0_log"
        mkdir -p $LOG_DIR
        export FULL_LOG_DIR="$(cd "$(dirname "$LOG_DIR")" && pwd)/$(basename "$LOG_DIR")"
        echo "FULL_LOG_DIR set to $FULL_LOG_DIR"
        bash log.sh --init

        run_and_echo docker-compose -f ./deployment/docker-compose-p${NODES}k1.yaml -f ./deployment/docker-compose-p${NODES}k1.override.yaml -p "${DOCKER_PROJECT}" up -d
        run_and_echo sleep 5
        run_and_echo docker ps -a

        # Determine container name for hybrid config verification
        container_name="${CONTAINER_NAME_PREFIX}_proton-server"
        if [ "${NODES}" != "1" ]; then
            container_name="${CONTAINER_NAME_PREFIX}_proton-server1"
        fi

        for field in "${HYBRID_FIELDS[@]}"; do
            if ! run_and_echo docker exec "${container_name}" grep -q "$field" "/etc/proton-server/config.yaml"; then
                MISSING_FIELD=$field
                run_and_echo docker exec "${container_name}" grep hybrid "/etc/proton-server/config.yaml"
                break
            fi
        done

        run_and_echo docker-compose -f ./deployment/docker-compose-p${NODES}k1.yaml -f ./deployment/docker-compose-p${NODES}k1.override.yaml -p "${DOCKER_PROJECT}" down -v
        run_and_echo sleep 10
        run_and_echo docker ps -a

        if [ -n "$MISSING_FIELD" ]; then
            echo "failed to set hybrid config: $MISSING_FIELD"
            exit 1
        else
            echo "succeeded to set hybrid config"
        fi
    fi
}

for feature in "${features[@]}"; do
    if [[ "$feature" == "hybrid" ]]; then
        AGGREGATION_CHECK_MODE=random
        OVERRIDE_FLAG="-f ./deployment/docker-compose-p${NODES}k1.override.yaml"
        add_to_blacklist "not_supported_hybrid"
        echo "Smoke test executed in hybrid mode"
    elif [[ "$feature" == "python-udf-library" ]]; then
        remove_from_blacklist "python_udf_library"
        echo "Smoke test scope contains: python-udf-library"
    else
        echo "Error: Invalid parameter '$feature'. support: [hybrid;python-udf-library]"
        break
    fi
done

if [ ${#BLACKLIST[@]} -gt 0 ]; then
    BLACKLIST_FLAG="-b $(IFS=, ; echo "${BLACKLIST[*]}")"
else
    BLACKLIST_FLAG=""
fi

if [ "$CI" == "true" ]; then
    export SSH_USER=ubuntu
    export SSH_DIR=/home/ubuntu/.ssh
fi

echo "CI:${CI}"
echo "SANITIZER:${SANITIZER}"
echo "AGGREGATION_CHECK_MODE:${AGGREGATION_CHECK_MODE}"
echo "OVERRIDE_FLAG:${OVERRIDE_FLAG}"
echo "BLACKLIST_FLAG:${BLACKLIST_FLAG}"

test_hybrid_config "$AGGREGATION_CHECK_MODE"

# prepare ssh keys
bash ./ssh.sh init
echo "run.sh SSH_DIR:${SSH_DIR}"

# prepare clickhouse certs
bash ./cert.sh

# get all names and concatenate them into a string
for folder in $(find "./smoke" -mindepth 1 -maxdepth 1 -type d ! -name "deployment"); do
    config_file="$folder/config.yaml"
    if [ -f "$config_file" ]; then
        name=$(awk '/^name:/ {print $2}' "$config_file")
        TOTAL_NAMES="$TOTAL_NAMES$name,"
    fi
done

set +e

echo "$TOTAL_NAMES" > temp.lock

# use 5 clusters to run the smoke tests
for INDEX in {1..5}; do
{
    ENV_VARIABLE="${GROUPS_ARRAY[$INDEX-1]%,}"
    export ENV_VARIABLE=$ENV_VARIABLE
    export CONTAINER_NAME_PREFIX=$INDEX
    export DOCKER_PROJECT="smoke_${INDEX}"
    

    export LOG_DIR=./log/p${NODES}k1/"$INDEX"_log
    mkdir -p $LOG_DIR
    
    while true; do
        (
        flock -x 7 7>&7 
        
        TOTAL_NAMES=$(cat temp.lock)
        # no task in the queue
        if [ -z "$TOTAL_NAMES" ]; then
            echo "1" > should_break.txt
        else
            echo "0" > should_break.txt
            SUITE_NAME=$(echo $TOTAL_NAMES | cut -d ',' -f 1)
            echo "$SUITE_NAME" > "$INDEX"_suite_name.txt
            TOTAL_NAMES=$(echo $TOTAL_NAMES | cut -d ',' -f 2-)
            
            echo "$TOTAL_NAMES" > temp.lock
        fi
        
        )7<>temp.lock

        SHOULD_BREAK=$(cat should_break.txt)
        if [ "$SHOULD_BREAK" -eq "1" ]; then
            echo "no task in queue, cancel new docker-compose"
            break
        fi

        export SUITE_NAME=$(cat "$INDEX"_suite_name.txt)
        PROFFIEL_FLAG=""
        CLUSTER="p${NODES}k1"
        if [ "$SUITE_NAME" == "dictionary" ]; then
            PROFFIEL_FLAG="--profile source"
        elif [ "$SUITE_NAME" == "http_external_stream" ]; then
            PROFFIEL_FLAG="--profile splunk"
        elif [ "$SUITE_NAME" == "external_table_mysql" ]; then
            PROFFIEL_FLAG="--profile mysql"
        elif [ "$SUITE_NAME" == "external_table_clickhouse" ]; then
            PROFFIEL_FLAG="--profile clickhouse"
        elif [ "$SUITE_NAME" == "external_table_postgres" ]; then
            PROFFIEL_FLAG="--profile postgres"
        elif [ "$SUITE_NAME" == "external_table_s3" ] || [ "$SUITE_NAME" == "storage" ]; then
            PROFFIEL_FLAG="--profile minio"
            CLUSTER="p${NODES}m1"
        else
            echo "test with default deploy"
        fi

        # clean and create proton log mounted volume
        export FULL_LOG_DIR="$(cd "$(dirname "$LOG_DIR")" && pwd)/$(basename "$LOG_DIR")"
        echo "FULL_LOG_DIR set to $FULL_LOG_DIR"
        bash log.sh --init
        sleep 2

        docker-compose -f ./deployment/docker-compose-p${NODES}k1.yaml $PROFFIEL_FLAG $OVERRIDE_FLAG -p "smoke_$INDEX" up -d
        sleep 15
        echo "start up proton on test suite: $SUITE_NAME"
        docker ps -a --filter "label=com.docker.compose.project=smoke_$INDEX"

        # get the suite name and run the smoke test
        mkdir -p "$LOG_DIR"/"$SUITE_NAME"
        echo "Group $INDEX, Suite $SUITE_NAME: begin"
        run_and_echo docker exec ${INDEX}_p${NODES}k1_probe timeout ${SUITE_TIMEOUT} probe smoke -v /smoke -d /deploy/smoke -c ${CLUSTER} -s ${SUITE_NAME} --os ${OS} --arch ${ARCH} --variable CONTAINER_NAME_PREFIX=${CONTAINER_NAME_PREFIX} --variable sanitizer=${SANITIZER} --variable aggregation_check_mode=${AGGREGATION_CHECK_MODE} --variable nodes=${NODES} ${BLACKLIST_FLAG} --retry ${MAX_RETRIES} -f /log/probe.log
        echo "Group $INDEX, Suite $SUITE_NAME: end"

        # print && collect proton log
        bash log.sh --fatal-log ./.fatal
        bash log.sh --fatal-log /artifacts/log/proton.log.fatal
        bash log.sh --output-dir /artifacts/log/${SUITE_NAME}

        # FD leak detection for this test suite
        echo "=== FD Leak Check for Suite: $SUITE_NAME ==="
        mkdir -p /artifacts/fd_diagnostics
        FD_COUNT=$(ls -l /proc/*/fd/* 2>/dev/null | grep deleted | wc -l)
        echo "Suite $SUITE_NAME - Deleted FD count: $FD_COUNT" >> /artifacts/fd_diagnostics/fd_per_suite_summary.txt
        ls -l /proc/*/fd/* 2>/dev/null | grep deleted > /artifacts/fd_diagnostics/fd_dump_${SUITE_NAME}.txt
        echo "Group $INDEX, Suite $SUITE_NAME: FD count after test: $FD_COUNT"

        # print && collect probe log
        grep -r "Gatherer" "$LOG_DIR"/probe.log >> ./log/summary.log

        if [ ! -f "$LOG_DIR"/.status ]; then
            # no .status means the probe encounters with a panic or probe timeout
            echo "Group $INDEX, Suite $SUITE_NAME: Failed with panic or timeout" >> ./.fail
            echo "Group $INDEX, Suite $SUITE_NAME: Failed with panic or timeout"
            echo "Panic Log:"
        elif grep -q "succeed" "$LOG_DIR"/.status; then
            cp "$LOG_DIR"/probe.log "$LOG_DIR"/"$SUITE_NAME"/succeed.log
            echo "Group $INDEX, Suite $SUITE_NAME: Succeeded"
            echo "Succeed Log:"
        elif [ ! -f "$LOG_DIR/.wrong" ]; then
            cp "$LOG_DIR"/probe.log "$LOG_DIR"/"$SUITE_NAME"/fail_without_wrong_case.log
            echo "Group $INDEX, Suite $SUITE_NAME: Fail with no wrong cases found, it's caused by unrelated setup or teardown"
            echo "Log:"
        else
            line=$(head -n 1 "$LOG_DIR"/.wrong)
            cp "$LOG_DIR"/probe.log "$LOG_DIR"/"$SUITE_NAME"/fail.log
            echo "Group $INDEX, Suite $SUITE_NAME: Failed after $MAX_RETRIES retries" >> ./.fail
            echo "Failed cases: $line" >> ./.fail

            echo "Group $INDEX, Suite $SUITE_NAME: Failed after $MAX_RETRIES retries"
            echo "Group $INDEX, Suite $SUITE_NAME Failed cases: $line"
            echo "Fail Log:"
        fi
        
        cat "$LOG_DIR"/probe.log

        docker-compose -f ./deployment/docker-compose-p${NODES}k1.yaml $OVERRIDE_FLAG -p "smoke_$INDEX" down -v

        sleep 10
        echo "terminate proton on test suite: $SUITE_NAME"
        docker ps -a --filter "label=com.docker.compose.project=smoke_$INDEX"
    done
    
    
}&
done

wait 

aws s3 cp --no-progress --recursive ./log s3://tp-internal2/proton-oss/cluster/enterprise

bash ./ssh.sh clean

# print disk usage after test
df -lh

# print all the summary logs
cat ./log/summary.log

echo "====================Smoke Testing Result Summary===================="
# check if any test failed
if [ -f ./.fatal ]; then
    cat ./.fatal >> ./.fail
    rm -f ./.fatal
fi

if [ -f ./.fail ]; then
    echo "Some tests failed"  | tee -a $GITHUB_STEP_SUMMARY

    fail_output=$(cat ./.fail)
    if [ ${#fail_output} -gt 900 ]; then
        cat ./.fail
        cat ./.fail | head -c 900 >> $GITHUB_STEP_SUMMARY
        echo "-" >> $GITHUB_STEP_SUMMARY
        echo "### Output is too large, truncated to 900 bytes." >> $GITHUB_STEP_SUMMARY
    else
        echo "$fail_output" | tee -a $GITHUB_STEP_SUMMARY
    fi
else
    echo "All tests succeeded"  | tee -a $GITHUB_STEP_SUMMARY
fi
echo "====================Smoke Testing Result Summary===================="

echo "====================Smoke Testing Fatal Log========================="
cat ./.fatal
echo "====================Smoke Testing Fatal Log========================="

echo "=== FD Leak Summary ==="
FINAL_FD_COUNT=$(ls -l /proc/*/fd/* 2>/dev/null | grep deleted | wc -l)
BASELINE_FD_COUNT=$(cat /artifacts/fd_diagnostics/fd_baseline.txt 2>/dev/null | grep -o '[0-9]*' || echo "0")
TOTAL_LEAK=$((FINAL_FD_COUNT - BASELINE_FD_COUNT))
echo "Final deleted FD count: $FINAL_FD_COUNT" >> /artifacts/fd_diagnostics/fd_final_summary.txt
echo "Baseline deleted FD count: $BASELINE_FD_COUNT" >> /artifacts/fd_diagnostics/fd_final_summary.txt  
echo "Total FD leak across all suites: $TOTAL_LEAK" >> /artifacts/fd_diagnostics/fd_final_summary.txt
cat /artifacts/fd_diagnostics/fd_final_summary.txt

if [ -f ./.fail ]; then
    rm -f ./.fail
    exit 1
fi
