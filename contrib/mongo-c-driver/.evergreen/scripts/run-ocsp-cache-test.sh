#!/usr/bin/env bash

# End-to-end test runner for OCSP cache.
#
# Assumptions:
#   Mongod:
#       The script assumes that a mongod is running with TLS enabled. It also assumes that the server certificate will NOT
#       staple a response. This will force the test binary to reach out to an OCSP responder to check the certificates'
#       revocation status.
#   Mock OCSP Responder:
#       This script assumes that a mock OCSP responder is running named "ocsp_mock". It also assumes that the OCSP
#       responder will respond with a certificate status of 'revoked'.
#
# Behavior:
#   This script first runs the test binary 'test-mongoc-cache' which sends a ping command to the mongod. It then waits for 5
#   seconds to give the binary enough time to make the request, and receive and process the response. Since we soft-fail
#   if an OCSP responder is not reachable, receiving a certificate status of 'revoked' is the only way we can be certain
#   our binary reached out to an OCSP responder. We assert a certificate status of 'revoked' in the test binary for both
#   ping commands.
#
#   The test binary will hang (it calls 'raise (SIGSTOP)') after the first ping. This gives us enough time to kill the
#   mock OCSP responder before sending the second ping command to the server. If the cache is used, the expected behavior,
#   then the binary will use the response cached from the first ping command and report a certificate status of 'revoked'.
#   However, if the cache is not used, then second ping command will attempt to reach out to an OCSP responder. Since the
#   only one available was killed by this script and we soft-fail if we cannot contact an OCSP responder the binary
#   will report a certificate status of "good".
#
#   The aforementioned behavior is asserted in the test binary, i.e., both ping commands should fail. If they do,
#   test-mongoc-cache will return EXIT_SUCCESS, otherwise, it will return EXIT_FAILURE.
#
# Environment variables:
#
# CERT_TYPE
#   Required. Set to either RSA or ECDSA.

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_req CERT_TYPE

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare openssl_install_dir="${mongoc_dir}/openssl-install-dir"

if ! pgrep -nf mongod >/dev/null; then
  echo "Cannot find mongod. See file comments for help." 1>&2
  exit 1
fi

if ! pgrep -nf ocsp_mock >/dev/null; then
  echo "Cannot find mock OCSP responder. See file comments for help." 1>&2
  exit 1
fi

# Custom OpenSSL library may be installed. Only prepend to LD_LIBRARY_PATH when
# necessary to avoid conflicting with system binary requirements.
declare openssl_lib_prefix="${LD_LIBRARY_PATH:-}"
if [[ -d "${openssl_install_dir}" ]]; then
  openssl_lib_prefix="${openssl_install_dir}/lib:${openssl_lib_prefix:-}"
fi

# This test will hang after the first ping.
LD_LIBRARY_PATH="${openssl_lib_prefix}" "${mongoc_dir}/src/libmongoc/test-mongoc-cache" "${mongoc_dir}/.evergreen/ocsp/${CERT_TYPE}/ca.pem" &
sleep 5 # Give the program time to contact the OCSP responder

pkill -nf "ocsp_mock" # We assume that the mock OCSP responder is named "ocsp_mock"

# Resume the test binary. This will cause it to send the second ping command.
kill -s SIGCONT "$(pgrep -nf test-mongoc-cache)"
