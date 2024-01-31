#!/bin/bash

set -eo pipefail
shopt -s nullglob

DO_CHOWN=1
if [ "${PROTON_DO_NOT_CHOWN:-0}" = "1" ]; then
    DO_CHOWN=0
fi

PROTON_UID="${PROTON_UID:-"$(id -u timeplus)"}"
PROTON_GID="${PROTON_GID:-"$(id -g timeplus)"}"

# support --user
if [ "$(id -u)" = "0" ]; then
    USER=$PROTON_UID
    GROUP=$PROTON_GID
    if command -v gosu &> /dev/null; then
        gosu="gosu $USER:$GROUP"
    elif command -v su-exec &> /dev/null; then
        gosu="su-exec $USER:$GROUP"
    else
        echo "No gosu/su-exec detected!"
        exit 1
    fi
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    gosu=""
    DO_CHOWN=0
fi

# set some vars
PROTON_CONFIG="${PROTON_CONFIG:-/etc/proton-server/config.yaml}"

if ! $gosu test -f "$PROTON_CONFIG" -a -r "$PROTON_CONFIG"; then
    echo "Configuration file '$PROTON_CONFIG' isn't readable by user with id '$USER'"
    exit 1
fi

# get `proton` directories locations
DATA_DIR=/var/lib/proton/
TMP_DIR=/var/lib/proton/tmp/
USER_PATH=/var/lib/proton/user_files/
LOG_PATH=/var/log/proton-server/proton-server.log
LOG_DIR="$(dirname "$LOG_PATH")"
ERROR_LOG_PATH=/var/log/proton-server/proton-server.err.log
ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH")"
FORMAT_SCHEMA_PATH=/var/lib/proton/format_schemas/

PROTON_USER="${PROTON_USER:-default}"
PROTON_PASSWORD="${PROTON_PASSWORD:-}"
PROTON_DB="${PROTON_DB:-}"
PROTON_ACCESS_MANAGEMENT="${PROTON_DEFAULT_ACCESS_MANAGEMENT:-0}"

for dir in "$DATA_DIR" \
  "$ERROR_LOG_DIR" \
  "$LOG_DIR" \
  "$TMP_DIR" \
  "$USER_PATH" \
  "$FORMAT_SCHEMA_PATH"
do
    # check if variable not empty
    [ -z "$dir" ] && continue
    # ensure directories exist
    if [ "$DO_CHOWN" = "1" ]; then
      mkdir="mkdir"
    else
      mkdir="$gosu mkdir"
    fi
    if ! $mkdir -p "$dir"; then
        echo "Couldn't create necessary directory: $dir"
        exit 1
    fi

    if [ "$DO_CHOWN" = "1" ]; then
        # ensure proper directories permissions
        # but skip it for if directory already has proper premissions, cause recursive chown may be slow
        if [ "$(stat -c %u "$dir")" != "$USER" ] || [ "$(stat -c %g "$dir")" != "$GROUP" ]; then
            chown -R "$USER:$GROUP" "$dir"
        fi
    elif ! $gosu test -d "$dir" -a -w "$dir" -a -r "$dir"; then
        echo "Necessary directory '$dir' isn't accessible by user with id '$USER'"
        exit 1
    fi
done

# if proton user is defined - create it (user "default" already exists out of box)
if [ -n "$PROTON_USER" ] && [ "$PROTON_USER" != "default" ] || [ -n "$PROTON_PASSWORD" ]; then
    echo "$0: create new user '$PROTON_USER' instead 'default'"
    cat <<EOT > /etc/proton-server/users.d/default-user.xml
    <clickhouse>
      <!-- Docs: <https://proton.tech/docs/en/operations/settings/settings_users/> -->
      <users>
        <!-- Remove default user -->
        <default remove="remove">
        </default>

        <${PROTON_USER}>
          <profile>default</profile>
          <networks>
            <ip>::/0</ip>
          </networks>
          <password>${PROTON_PASSWORD}</password>
          <quota>default</quota>
          <access_management>${PROTON_ACCESS_MANAGEMENT}</access_management>
        </${PROTON_USER}>
      </users>
    </clickhouse>
EOT
fi

if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$PROTON_DB" ]; then
    # port is needed to check if proton-server is ready for connections
    HTTP_PORT=8123

    # Listen only on localhost until the initialization is done
    $gosu /usr/bin/proton-server --config-file="$PROTON_CONFIG" -- --listen_host=127.0.0.1 &
    pid="$!"

    # check if proton is ready to accept connections
    # will try to send ping proton via http_port (max 12 retries by default, with 1 sec timeout and 1 sec delay between retries)
    tries=${PROTON_INIT_TIMEOUT:-12}
    while ! wget --spider -T 1 -q "http://127.0.0.1:$HTTP_PORT/ping" 2>/dev/null; do
        if [ "$tries" -le "0" ]; then
            echo >&2 'Proton init process failed.'
            exit 1
        fi
        tries=$(( tries-1 ))
        sleep 1
    done

    protonclient=( proton-client --multiquery --host "127.0.0.1" -u "$PROTON_USER" --password "$PROTON_PASSWORD" )

    echo

    # create default database, if defined
    if [ -n "$PROTON_DB" ]; then
        echo "$0: create database '$PROTON_DB'"
        "${protonclient[@]}" -q "CREATE DATABASE IF NOT EXISTS $PROTON_DB";
    fi

    for f in /docker-entrypoint-initdb.d/*; do
        case "$f" in
            *.sh)
                if [ -x "$f" ]; then
                    echo "$0: running $f"
                    "$f"
                else
                    echo "$0: sourcing $f"
                    # shellcheck source=/dev/null
                    . "$f"
                fi
                ;;
            *.sql)    echo "$0: running $f"; "${protonclient[@]}" < "$f" ; echo ;;
            *.sql.gz) echo "$0: running $f"; gunzip -c "$f" | "${protonclient[@]}"; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo
    done

    if ! kill -s TERM "$pid" || ! wait "$pid"; then
        echo >&2 'Finishing of proton init process failed.'
        exit 1
    fi
fi

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
    exec $gosu /usr/bin/proton-server --config-file="$PROTON_CONFIG" "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
