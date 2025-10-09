#!/bin/bash

set -euxf -o pipefail

export MINIO_ROOT_USER=${MINIO_ROOT_USER:-clickhouse}
export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-clickhouse}
MINIO_SERVER_VERSION=${MINIO_SERVER_VERSION:-2024-08-03T04-33-23Z}
MINIO_CLIENT_VERSION=${MINIO_CLIENT_VERSION:-2024-07-31T15-58-33Z}

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define MINIO_DIR as the minio folder under the script directory
MINIO_DIR="$SCRIPT_DIR/minio"

export MINIO_CONFIG_DIR="$MINIO_DIR/config/"  # Define custom configuration file path
# Create MINIO_DIR if it doesn't exist
mkdir -p "$MINIO_DIR"

download_minio() {
  local minio_bin="$MINIO_DIR/minio"
  local mc_bin="$MINIO_DIR/mc"
  if [ -f "$minio_bin" ] && [ -f "$mc_bin" ]; then
    echo "MinIO binaries already exist, skipping download."
    return
  fi
  local os=$(uname -s | tr '[:upper:]' '[:lower:]')
  local arch=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
  wget "https://dl.min.io/server/minio/release/${os}-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -O "$minio_bin"
  wget "https://dl.min.io/client/mc/release/${os}-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -O "$mc_bin"
  chmod +x "$mc_bin" "$minio_bin"
}

start_minio() {
  "$MINIO_DIR/minio" --version
  "$MINIO_DIR/minio" server --address ":11111" "$MINIO_DIR/minio_data" &
  echo $! > "$MINIO_DIR/minio.pid"  # Save PID
  wait_for_it
  lsof -i :11111
  sleep 5
}

wait_for_it() {
  local counter=0
  local max_counter=60
  local url="http://localhost:11111"
  local params=(
    --silent
    --verbose
  )
  while ! curl "${params[@]}" "${url}" 2>&1 | grep AccessDenied
  do
    if [[ ${counter} == "${max_counter}" ]]; then
      echo "Failed to start MinIO."
      exit 0
    fi
    echo "Attempting to connect to MinIO..."
    sleep 1
    counter=$((counter + 1))
  done
}

setup_minio() {
  "$MINIO_DIR/mc" alias set clickminio http://localhost:11111 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
  "$MINIO_DIR/mc" admin user add clickminio proton protonproton
  "$MINIO_DIR/mc" admin policy attach clickminio readwrite --user=proton 2>/dev/null
  "$MINIO_DIR/mc" mb --ignore-existing clickminio/test
  "$MINIO_DIR/mc" anonymous set public clickminio/test
  setup_aws_credentials
}

setup_aws_credentials() {
  mkdir -p ~/.aws
  cat <<EOT > ~/.aws/credentials
[default]
aws_access_key_id=${MINIO_ROOT_USER}
aws_secret_access_key=${MINIO_ROOT_PASSWORD}
EOT
}

upload_data() {
  local query_dir="$1"
  local test_path="${2:-$SCRIPT_DIR/$query_dir/data_minio}"

  if [ -d "$test_path" ]; then
    "$MINIO_DIR/mc" cp --recursive "$test_path"/ clickminio/test/
  fi
}

stop_minio() {
  if [ -f "$MINIO_DIR/minio.pid" ]; then
    kill "$(cat "$MINIO_DIR/minio.pid")" && rm -f "$MINIO_DIR/minio.pid"
    echo "MinIO service has been stopped."
  else
    echo "MinIO PID file not found. Trying to stop by process name."
    pkill -f "$MINIO_DIR/minio server --address :11111" || echo "MinIO service is not running."
  fi
}

clean_minio() {
  stop_minio
  rm -rf "$MINIO_DIR/minio_data" "$MINIO_DIR/minio.pid"
  echo "MinIO data has been deleted."
}

main() {
  case "$1" in
    start)
      shift
      local data_dir="${1:-}"
      download_minio
      start_minio "$data_dir"
      setup_minio
      echo "The path of test_path is ${data_dir:-}"
      upload_data "0_stateless" "${data_dir:-}"
      ;;
    stop)
      stop_minio
      ;;
    clean)
      clean_minio
      ;;
    *)
      echo "Usage: $0 {start [data_dir]|stop|clean}"
      exit 1
      ;;
  esac
}

main "$@"

