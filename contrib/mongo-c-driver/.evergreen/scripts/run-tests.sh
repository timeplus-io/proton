#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_opt ASAN "OFF"
check_var_opt AUTH "noauth"
check_var_opt CC
check_var_opt CLIENT_SIDE_ENCRYPTION
check_var_opt COMPRESSORS "nocompressors"
check_var_opt COVERAGE # CMake default: OFF.
check_var_opt DNS "nodns"
check_var_opt IPV4_ONLY
check_var_opt LOADBALANCED "noloadbalanced"
check_var_opt MARCH
check_var_opt MONGODB_API_VERSION
check_var_opt MULTI_MONGOS_LB_URI
check_var_opt SINGLE_MONGOS_LB_URI
check_var_opt SKIP_CRYPT_SHARED_LIB
check_var_opt SSL "nossl"
check_var_opt URI

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare openssl_install_dir="${mongoc_dir}/openssl-install-dir"

if [[ "${COMPRESSORS}" != "nocompressors" ]]; then
  export MONGOC_TEST_COMPRESSORS="${COMPRESSORS}"
fi
if [[ "${AUTH}" != "noauth" ]]; then
  export MONGOC_TEST_USER="bob"
  export MONGOC_TEST_PASSWORD="pwd123"
fi

if [[ "${SSL}" != "nossl" ]]; then
  export MONGOC_TEST_SSL_WEAK_CERT_VALIDATION="off"
  export MONGOC_TEST_SSL_PEM_FILE="src/libmongoc/tests/x509gen/client.pem"
  export MONGOC_TEST_SSL_CA_FILE="src/libmongoc/tests/x509gen/ca.pem"
fi

export MONGOC_ENABLE_MAJORITY_READ_CONCERN=on
export MONGOC_TEST_FUTURE_TIMEOUT_MS=30000
export MONGOC_TEST_URI="${URI}"
export MONGOC_TEST_SERVER_LOG="json"
export MONGOC_TEST_SKIP_MOCK="on"
export MONGOC_TEST_IPV4_AND_IPV6_HOST="ipv4_and_ipv6.test.build.10gen.cc"

if [[ "${IPV4_ONLY}" != "on" ]]; then
  export MONGOC_CHECK_IPV6="on"
fi

# Only set creds if testing with Client Side Encryption.
# libmongoc may build with CSE enabled (if the host has libmongocrypt installed)
# and will try to run those tests (which fail on ASAN unless spawning is bypassed).
if [[ -n "${CLIENT_SIDE_ENCRYPTION}" ]]; then
  echo "Testing with Client Side Encryption enabled."

  echo "Setting temporary credentials..."
  pushd "${mongoc_dir}/../drivers-evergreen-tools/.evergreen/csfle"
  {
    export AWS_SECRET_ACCESS_KEY="${client_side_encryption_aws_secret_access_key:?}"
    export AWS_ACCESS_KEY_ID="${client_side_encryption_aws_access_key_id:?}"
    export AWS_DEFAULT_REGION="us-east-1"
  } &>/dev/null
  echo "Running activate-kmstlsvenv.sh..."
  # shellcheck source=/dev/null
  . ./activate-kmstlsvenv.sh
  echo "Running activate-kmstlsvenv.sh... done."
  echo "Running set-temp-creds.sh..."
  # shellcheck source=/dev/null
  . ./set-temp-creds.sh
  echo "Running set-temp-creds.sh... done."
  deactivate
  popd # "${mongoc_dir}/../drivers-evergreen-tools/.evergreen/csfle"
  echo "Setting temporary credentials... done."

  # Ensure temporary credentials were properly set.
  if [ -z "${CSFLE_AWS_TEMP_ACCESS_KEY_ID}" ]; then
    echo "Failed to set temporary credentials!" 1>&2
    exit 1
  fi

  echo "Setting KMS credentials from the environment..."
  {
    export MONGOC_TEST_AWS_TEMP_ACCESS_KEY_ID="${CSFLE_AWS_TEMP_ACCESS_KEY_ID:?}"
    export MONGOC_TEST_AWS_TEMP_SECRET_ACCESS_KEY="${CSFLE_AWS_TEMP_SECRET_ACCESS_KEY:?}"
    export MONGOC_TEST_AWS_TEMP_SESSION_TOKEN="${CSFLE_AWS_TEMP_SESSION_TOKEN:?}"
    export MONGOC_TEST_AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
    export MONGOC_TEST_AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
    export MONGOC_TEST_AWSNAME2_SECRET_ACCESS_KEY="${client_side_encryption_awsname2_secret_access_key}"
    export MONGOC_TEST_AWSNAME2_ACCESS_KEY_ID="${client_side_encryption_awsname2_access_key_id}"
    export MONGOC_TEST_AZURE_TENANT_ID="${client_side_encryption_azure_tenant_id:?}"
    export MONGOC_TEST_AZURE_CLIENT_ID="${client_side_encryption_azure_client_id:?}"
    export MONGOC_TEST_AZURE_CLIENT_SECRET="${client_side_encryption_azure_client_secret:?}"
    export MONGOC_TEST_GCP_EMAIL="${client_side_encryption_gcp_email:?}"
    export MONGOC_TEST_GCP_PRIVATEKEY="${client_side_encryption_gcp_privatekey:?}"
  } &>/dev/null
  echo "Setting KMS credentials from the environment... done."

  export MONGOC_TEST_CSFLE_TLS_CA_FILE="src/libmongoc/tests/x509gen/ca.pem"
  export MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE="src/libmongoc/tests/x509gen/client.pem"
  export SKIP_CRYPT_SHARED_LIB="${SKIP_CRYPT_SHARED_LIB}"
fi

# Sanitizer environment variables.
export ASAN_OPTIONS="detect_leaks=1 abort_on_error=1 symbolize=1"
export ASAN_SYMBOLIZER_PATH="/opt/mongodbtoolchain/v3/bin/llvm-symbolizer"
export TSAN_OPTIONS="suppressions=./.tsan-suppressions"
export UBSAN_OPTIONS="print_stacktrace=1 abort_on_error=1"

declare -a test_args=(
  "-d"
  "-F"
  "test-results.json"
  "--skip-tests"
  ".evergreen/etc/skip-tests.txt"
)

# TODO (CDRIVER-4045): consolidate DNS tests into regular test tasks.
if [[ "${DNS}" != "nodns" ]]; then
  if [[ "${CC}" =~ mingw ]]; then
    echo "ERROR - DNS tests not implemented for MinGW yet" 1>&2
    exit 1
  fi

  test_args+=("-l" "/initial_dns_seedlist_discovery/*")

  if [[ "${DNS}" = "loadbalanced" ]]; then
    export MONGOC_TEST_DNS_LOADBALANCED=on
  else
    export MONGOC_TEST_DNS=on
  fi
fi

wait_for_server() {
  declare name="${1:?"wait_for_server requires a server name"}"
  declare port="${2:?"wait_for_server requires a server port"}"

  for _ in $(seq 300); do
    # Exit code 7: "Failed to connect to host".
    if
      curl -s --max-time 1 "localhost:${port}" >/dev/null
      test ${?} -ne 7
    then
      return 0
    else
      sleep 1
    fi
  done
  echo "Could not detect ${name} server on port ${port}" 1>&2
  return 1
}

if [[ "${CC}" =~ mingw ]]; then
  echo "Waiting for simple HTTP server to start..."
  wait_for_server "simple HTTP" 8000
  echo "Waiting for simple HTTP server to start... done."

  chmod -f +x ./src/libmongoc/test-libmongoc.exe
  cmd.exe /c "$(native-path "${script_dir}/run-tests-mingw.bat")"
  exit
fi

# shellcheck source=.evergreen/scripts/add-build-dirs-to-paths.sh
. "${script_dir}/add-build-dirs-to-paths.sh"
# shellcheck source=.evergreen/scripts/bypass-dlclose.sh
. "${script_dir}/bypass-dlclose.sh"

check_mongocryptd() {
  if [[ "${CLIENT_SIDE_ENCRYPTION}" == "on" && "${ASAN}" == "on" ]]; then
    # ASAN does not play well with spawned processes. In addition to --no-fork, do not spawn mongocryptd
    # for client-side encryption tests.
    export MONGOC_TEST_MONGOCRYPTD_BYPASS_SPAWN="on"
    mongocryptd --logpath ./mongocryptd.logs --fork --pidfilepath="$(pwd)/mongocryptd.pid"
  fi
}

export MONGOC_TEST_MONITORING_VERBOSE=on

# Limit tests to execute and ensure required servers are running.
if [[ "${CLIENT_SIDE_ENCRYPTION}" == "on" ]]; then
  echo "Waiting for mock KMS servers to start..."
  wait_for_server "mock KMS" 8999
  wait_for_server "mock KMS" 9000
  wait_for_server "mock KMS" 9001
  wait_for_server "mock KMS" 9002
  wait_for_server "mock KMIP" 5698
  echo "Waiting for mock KMS servers to start... done."

  # Check if tests should use the crypt_shared library.
  if [[ "${SKIP_CRYPT_SHARED_LIB}" == "on" ]]; then
    echo "crypt_shared library is skipped due to SKIP_CRYPT_SHARED_LIB=on"
  elif [[ -d /cygdrive/c ]]; then
    # We have trouble with this test on Windows. only set cryptSharedLibPath on other platforms
    echo "crypt_shared library is skipped due to running on Windows"
  else
    export MONGOC_TEST_CRYPT_SHARED_LIB_PATH="${CRYPT_SHARED_LIB_PATH}"
    echo "crypt_shared library will be loaded with cryptSharedLibPath: [${MONGOC_TEST_CRYPT_SHARED_LIB_PATH}]"
  fi

  # Limit tests executed to CSE tests.
  test_args+=("-l" "/client_side_encryption/*")
fi

if [[ "${LOADBALANCED}" != "noloadbalanced" ]]; then
  if [[ -z "${SINGLE_MONGOS_LB_URI}" || -z "${MULTI_MONGOS_LB_URI}" ]]; then
    echo "SINGLE_MONGOS_LB_URI and MULTI_MONGOS_LB_URI environment variables required." 1>&2
    exit 1
  fi

  # Limit tests executed to load balancer tests.
  export MONGOC_TEST_LOADBALANCED=ON

  # Limit tests executed to load balancer tests.
  test_args+=("-l" "/unified/*")
  test_args+=("-l" "/retryable_reads/*")
  test_args+=("-l" "/retryable_writes/*")
  test_args+=("-l" "/change_streams/*")
  test_args+=("-l" "/loadbalanced/*")
  test_args+=("-l" "/load_balancers/*")
  test_args+=("-l" "/crud/unified/*")
  test_args+=("-l" "/transactions/unified/*")
  test_args+=("-l" "/collection-management/*")
  test_args+=("-l" "/sessions/unified/*")
  test_args+=("-l" "/change_streams/unified/*")
  test_args+=("-l" "/versioned_api/*")
  test_args+=("-l" "/command_monitoring/unified/*")
fi

if [[ ! "${test_args[*]}" =~ "-l" ]]; then
  # /http tests are only run if the set of tests to execute were not limited.
  echo "Waiting for simple HTTP server to start..."
  wait_for_server "simple HTTP" 18000
  echo "Waiting for simple HTTP server to start... done."
fi

declare ld_preload="${LD_PRELOAD:-}"
if [[ "${ASAN}" == "on" ]]; then
  ld_preload="$(bypass_dlclose):${ld_preload}"
fi

case "${OSTYPE}" in
cygwin)
  export PATH
  PATH+=":/cygdrive/c/mongodb/bin"
  PATH+=":/cygdrive/c/libmongocrypt/bin"

  check_mongocryptd

  chmod -f +x src/libmongoc/Debug/test-libmongoc.exe
  LD_PRELOAD="${ld_preload:-}" ./src/libmongoc/Debug/test-libmongoc.exe "${test_args[@]}"
  ;;

*)
  ulimit -c unlimited || true
  # Need mongocryptd on the path.
  export PATH
  PATH+=":$(pwd)/mongodb/bin"
  check_mongocryptd

  # Custom OpenSSL library may be installed. Only prepend to LD_LIBRARY_PATH
  # when necessary to avoid conflicting with system binary requirements.
  declare openssl_lib_prefix="${LD_LIBRARY_PATH:-}"
  if [[ -d "${openssl_install_dir}" ]]; then
    openssl_lib_prefix="${openssl_install_dir}/lib:${openssl_lib_prefix:-}"
  fi

  LD_LIBRARY_PATH="${openssl_lib_prefix}" LD_PRELOAD="${ld_preload:-}" ./src/libmongoc/test-libmongoc --no-fork "${test_args[@]}"
  ;;
esac

if [[ "${COVERAGE}" == "ON" ]]; then
  declare -a coverage_args=(
    "--capture"
    "--derive-func-data"
    "--directory"
    "."
    "--output-file"
    ".coverage.lcov"
    "--no-external"
  )

  {
    case "${CC}" in
    clang)
      lcov --gcov-tool "$(pwd)/.evergreen/scripts/llvm-gcov.sh" "${coverage_args[@]}"
      ;;
    *)
      lcov --gcov-tool gcov "${coverage_args[@]}"
      ;;
    esac

    genhtml .coverage.lcov --legend --title "mongoc code coverage" --output-directory coverage
  } | perl -lne 'print if not m|Processing |'
fi
