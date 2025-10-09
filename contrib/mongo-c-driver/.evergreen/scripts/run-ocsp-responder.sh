#!/usr/bin/env bash

# Run an OCSP mock responder server if necessary.
#
# See the tests described in the specification for more info:
# https://github.com/mongodb/specifications/tree/master/source/ocsp-support/tests#integration-tests-permutations-to-be-tested.
# Precondition: mongod is NOT running. The responder should be started first.
#
# Environment variables:
#
# TEST_COLUMN
#   Required. Corresponds to a column of the test matrix. Set to one of the following:
#   TEST_1, TEST_2, TEST_3, TEST_4, SOFT_FAIL_TEST, MALICIOUS_SERVER_TEST_1, MALICIOUS_SERVER_TEST_2
# CERT_TYPE
#   Required. Set to either rsa or ecdsa.
# USE_DELEGATE
#   Required. May be ON or OFF. If a test requires use of a responder, this decides whether
#   the responder uses a delegate certificate. Defaults to "OFF"
#
# Example:
# TEST_COLUMN=TEST_1 CERT_TYPE=rsa USE_DELEGATE=OFF ./run-ocsp-test.sh
#

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_req TEST_COLUMN
check_var_req CERT_TYPE
check_var_req USE_DELEGATE

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare responder_required
case "${TEST_COLUMN}" in
TEST_1) responder_required="valid" ;;
TEST_2) responder_required="invalid" ;;
TEST_3) responder_required="valid" ;;
TEST_4) responder_required="invalid" ;;
MALICIOUS_SERVER_TEST_1) responder_required="invalid" ;;
esac

# Same responder is used for both server and client. So even stapling tests require a responder.

if [[ -n "${responder_required:-}" ]]; then
  echo "Starting mock responder"
  pushd ../drivers-evergreen-tools/.evergreen/ocsp
  # shellcheck source=/dev/null
  . ./activate-ocspvenv.sh
  popd # ../drivers-evergreen-tools/.evergreen/ocsp

  pushd "${mongoc_dir}/.evergreen/ocsp/${CERT_TYPE}"

  declare -a fault_args
  if [ "${responder_required}" == "invalid" ]; then
    fault_args=("--fault" "revoked")
  fi

  declare responder_signer
  if [[ "${USE_DELEGATE}" == "ON" ]]; then
    responder_signer="ocsp-responder"
  else
    responder_signer="ca"
  fi

  python ../ocsp_mock.py \
    --ca_file ca.pem \
    --ocsp_responder_cert "${responder_signer}.crt" \
    --ocsp_responder_key "${responder_signer}.key" \
    -p 8100 -v "${fault_args[@]}" \
    >"${mongoc_dir}/responder.log" 2>&1 &

  popd # "${mongoc_dir}/.evergreen/ocsp/${CERT_TYPE}"
fi
