#!/bin/bash

set -eo pipefail
shopt -s nullglob

# set some vars
PROTON_CONFIG="${PROTON_CONFIG:-/etc/proton-server/config.yaml}"

if ! test -f "$PROTON_CONFIG" -a -r "$PROTON_CONFIG"; then
    echo "Error: Cannot read configuration file '$PROTON_CONFIG'"
    echo "This file is only accessible by the 'timeplus' user (UID 101)."
    echo "This container is designed to run as UID 101 by default."
    echo "If you're using '--user' flag or 'runAsUser' in Kubernetes, please ensure it's set to 101."
    exit 1
fi

# get `proton` directories locations
PYTHON_SITE_PACKAGE_PATH=/var/lib/proton/python/lib/python3.10/site-packages/

PROTON_USER="${PROTON_USER:-default}"
PROTON_PASSWORD="${PROTON_PASSWORD:-}"
PROTON_ACCESS_MANAGEMENT="${PROTON_DEFAULT_ACCESS_MANAGEMENT:-0}"

for dir in "$PYTHON_SITE_PACKAGE_PATH"
do
    # check if variable not empty
    [ -z "$dir" ] && continue
    # ensure directories exist
    if ! mkdir -p "$dir"; then
        echo "Couldn't create necessary directory: $dir"
        exit 1
    fi

    if ! test -d "$dir" -a -w "$dir" -a -r "$dir"; then
        echo "Necessary directory '$dir' isn't accessible current user"
        exit 1
    fi
done

if [ -n "$STREAM_STORAGE_BROKERS" ]; then
    # Replace `brokers: localhost:9092` in config.yaml with customized one
    sed -i"" "s/brokers: localhost:9092/brokers: $STREAM_STORAGE_BROKERS/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup stream storage brokers.'
        exit 1
    fi
fi

# Produce latency
if [ -n "$STREAM_STORAGE_QUEUE_BUFFERING_MAX_MS" ]; then
    # Replace `queue_buffering_max_ms: 50` in config.yaml with customized one
    sed -i"" "s/queue_buffering_max_ms: 50\s*\(#.*\)\?$/queue_buffering_max_ms: $STREAM_STORAGE_QUEUE_BUFFERING_MAX_MS \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup queue_buffering_max_ms for stream storage.'
        exit 1
    fi
fi

if [ -n "$STREAM_STORAGE_FETCH_WAIT_MAX_MS" ]; then
    # Replace `fetch_wait_max_ms: 500` in config.yaml with customized one
    sed -i"" "s/fetch_wait_max_ms: 500\s*\(#.*\)\?$/fetch_wait_max_ms: $STREAM_STORAGE_FETCH_WAIT_MAX_MS \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup fetch_wait_max_ms for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_CONCURRENT_QUERIES" ]; then
    # Replace `max_concurrent_queries: 100` in config.yaml with customized one
    sed -i"" "s/max_concurrent_queries: 100\s*\(#.*\)\?$/max_concurrent_queries: $MAX_CONCURRENT_QUERIES \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup max_concurrent_queries for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_CONCURRENT_SELECT_QUERIES" ]; then
    # Replace `max_concurrent_select_queries: 100` in config.yaml with customized one
    sed -i"" "s/max_concurrent_select_queries: 100\s*\(#.*\)\?$/max_concurrent_select_queries: $MAX_CONCURRENT_SELECT_QUERIES \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup max_concurrent_select_queries for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_CONCURRENT_INSERT_QUERIES" ]; then
    # Replace `max_concurrent_insert_queries: 100` in config.yaml with customized one
    sed -i"" "s/max_concurrent_insert_queries: 100\s*\(#.*\)\?$/max_concurrent_insert_queries: $MAX_CONCURRENT_INSERT_QUERIES \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup max_concurrent_insert_queries for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_CONCURRENT_STREAMING_QUERIES" ]; then
    # Replace `streaming_processing_pool_size: 100` in config.yaml with customized one
    sed -i"" "s/streaming_processing_pool_size: 100\s*\(#.*\)\?$/streaming_processing_pool_size: $MAX_CONCURRENT_STREAMING_QUERIES \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup streaming_processing_pool_size for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_SERVER_MEMORY_USAGE_TO_RAM_RATIO" ]; then
    # Replace `max_server_memory_usage_to_ram_ratio: 0.9` in config.yaml with customized one
    sed -i"" "s/max_server_memory_usage_to_ram_ratio: 0.9\s*\(#.*\)\?$/max_server_memory_usage_to_ram_ratio: $MAX_SERVER_MEMORY_USAGE_TO_RAM_RATIO \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup max_server_memory_usage_to_ram_ratio for stream storage.'
        exit 1
    fi
fi

if [ -n "$MAX_SERVER_MEMORY_CACHE_TO_RAM_RATIO" ]; then
    # Replace `cache_size_to_ram_max_ratio: 0.5` in config.yaml with customized one
    sed -i"" "s/cache_size_to_ram_max_ratio: 0.5\s*\(#.*\)\?$/cache_size_to_ram_max_ratio: $MAX_SERVER_MEMORY_CACHE_TO_RAM_RATIO \1/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup cache_size_to_ram_max_ratio for stream storage.'
        exit 1
    fi
fi

if [ "$STREAM_STORAGE_TYPE" = "kafka" ]; then
    sed -i"" "/kafka:/{n;s/enabled: false/enabled: true/g}" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to enable kafka log.'
        exit 1
    fi

    sed -i"" "/nativelog:/{n;s/enabled: true/enabled: false/g}" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to disable native log.'
        exit 1
    fi

    if [ -n "$STREAM_STORAGE_SECURITY_PROTOCOL" ]; then
        sed -i"" "s/security_protocol: PLAINTEXT/security_protocol: $STREAM_STORAGE_SECURITY_PROTOCOL/g" "$PROTON_CONFIG"
        if [[ $? -ne 0 ]]; then
            echo >&2 'Failed to set kafka security protocol.'
            exit 1
        fi

        if [ -n "$STREAM_STORAGE_USERNAME" ]; then
            sed -i"" "s/# username:/username: $STREAM_STORAGE_USERNAME/g" "$PROTON_CONFIG"
            if [[ $? -ne 0 ]]; then
                echo >&2 'Failed to set kafka username.'
                exit 1
            fi
        fi

        if [ -n "$STREAM_STORAGE_PASSWORD" ]; then
            sed -i"" "s/# password:/password: $STREAM_STORAGE_PASSWORD/g" "$PROTON_CONFIG"
            if [[ $? -ne 0 ]]; then
                echo >&2 'Failed to set kafka password.'
                exit 1
            fi
        fi

        if [ -n "$STREAM_STORAGE_CA_CERT_FILE" ]; then
            sed -i"" "s/# ssl_ca_cert_file:/ssl_ca_cert_file: $STREAM_STORAGE_CA_CERT_FILE/g" "$PROTON_CONFIG"
            if [[ $? -ne 0 ]]; then
                echo >&2 'Failed to set ca cert file.'
                exit 1
            fi
        fi
    fi

    if [ -n "$STREAM_STORAGE_CLUSTER_ID" ]; then
        sed -i"" "s/cluster_id: default-sys-kafka-cluster-id/cluster_id: $STREAM_STORAGE_CLUSTER_ID/g" "$PROTON_CONFIG"
        if [[ $? -ne 0 ]]; then
            echo >&2 'Failed to set kafka cluster id.'
            exit 1
        fi
    fi

    if [ -n "$STREAM_STORAGE_LOGSTORE_REPLICATION_FACTOR" ]; then
        sed -i"" "s/logstore_replication_factor: 1\s*\(#.*\)\?$/logstore_replication_factor: $STREAM_STORAGE_LOGSTORE_REPLICATION_FACTOR \1/g" "$PROTON_CONFIG"
        if [[ $? -ne 0 ]]; then
            echo >&2 'Failed to set kafka logstore_replication_factor.'
            exit 1
        fi
    fi
elif [ "$STREAM_STORAGE_TYPE" = "nativelog" ]; then
    sed -i"" "/kafka:/{n;s/enabled: true/enabled: false/g}" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to disable kafka log.'
        exit 1
    fi

    sed -i"" "/nativelog:/{n;s/enabled: false/enabled: true/g}" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to enable native log.'
        exit 1
    fi
fi

if [ -n "$ENABLE_LOG_STREAM" ]; then
    # Replace `_tp_enable_log_stream_expr: false` in config.yaml with customized one
    sed -i"" "s/_tp_enable_log_stream_expr: false/_tp_enable_log_stream_expr: $ENABLE_LOG_STREAM/g" "$PROTON_CONFIG"
    if [[ $? -ne 0 ]]; then
        echo >&2 'Failed to setup _tp_enable_log_stream_expr.'
        exit 1
    fi
fi

# if no args passed to `docker run` or first argument start with `--`, then the user is passing proton-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    # Watchdog is launched by default, but does not send SIGINT to the main process,
    # so the container can't be finished by ctrl+c
    PROTON_WATCHDOG_ENABLE=${PROTON_WATCHDOG_ENABLE:-0}
    export PROTON_WATCHDOG_ENABLE
    exec /usr/bin/proton-server --config-file="$PROTON_CONFIG" "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
