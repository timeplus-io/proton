#!/usr/bin/env bash

# Test runner for AWS authentication.
#
# This script is meant to be run in parts (so to isolate the AWS tests).
# Pass the desired test as the environment variable TESTCASE: (REGULAR, EC2, ECS, ASSUME_ROLE, LAMBDA)
#
# Example:
# TESTCASE=EC2 run-aws-tests.sh


set -o errexit
set -o pipefail

# Do not trace
set +o xtrace

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
# shellcheck source=.evergreen/scripts/use-tools.sh
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_req TESTCASE

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare drivers_tools_dir
drivers_tools_dir="$(to_absolute "${mongoc_dir}/../drivers-evergreen-tools")"

declare test_awsauth="${mongoc_dir}/src/libmongoc/test-awsauth"

if [[ "${OSTYPE}" == "cygwin" ]]; then
  test_awsauth="${mongoc_dir}/src/libmongoc/Debug/test-awsauth.exe"
fi

expect_success() {
  echo "Should succeed:"
  "${test_awsauth}" "${1:?}" "EXPECT_SUCCESS" || exit
}

expect_failure() {
  echo "Should fail:"
  "${test_awsauth}" "${1:?}" "EXPECT_FAILURE" || exit
}


if [[ "${TESTCASE}" == "REGULAR" ]]; then
  echo "===== Testing regular auth via URI ====="

  # Create user on $external db.
  pushd "${drivers_tools_dir}/.evergreen/auth_aws"
  # shellcheck source=/dev/null
  . aws_setup.sh regular # Sets USER and PASS
  : "${USER:?}" "${PASS:?}"
  popd # "${drivers_tools_dir}/.evergreen/auth_aws"

  expect_success "mongodb://${USER:?}:${PASS:?}@localhost/?authMechanism=MONGODB-AWS"
  expect_failure "mongodb://${USER:?}:bad_password@localhost/?authMechanism=MONGODB-AWS"

  exit
fi

if [[ "${TESTCASE}" == "ASSUME_ROLE" ]]; then
  echo "===== Testing auth with session token via URI with AssumeRole ====="
  pushd "${drivers_tools_dir}/.evergreen/auth_aws"
  # shellcheck source=/dev/null
  . aws_setup.sh assume-role # Sets USER, PASS, and SESSION_TOKEN
  : "${USER:?}" "${PASS:?}" "${SESSION_TOKEN:?}"
  popd # "${drivers_tools_dir}/.evergreen/auth_aws"

  expect_success "mongodb://${USER}:${PASS}@localhost/aws?authMechanism=MONGODB-AWS&authSource=\$external&authMechanismProperties=AWS_SESSION_TOKEN:${SESSION_TOKEN}"
  expect_failure "mongodb://${USER}:${PASS}@localhost/aws?authMechanism=MONGODB-AWS&authSource=\$external&authMechanismProperties=AWS_SESSION_TOKEN:bad_token"
  exit
fi

if [[ "LAMBDA" = "$TESTCASE" ]]; then
  (
    echo "===== Testing auth via environment variables without session token ====="
    pushd "${drivers_tools_dir}/.evergreen/auth_aws"
    # shellcheck source=/dev/null
    . aws_setup.sh env-creds # Sets AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    : "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
    popd # "${drivers_tools_dir}/.evergreen/auth_aws"
    expect_success "mongodb://localhost/?authMechanism=MONGODB-AWS"
  )
  (
    echo "===== Testing auth via environment variables with session token ====="
    pushd "${drivers_tools_dir}/.evergreen/auth_aws"
    # shellcheck source=/dev/null
    . aws_setup.sh session-creds # Sets AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN
    : "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}" "${AWS_SESSION_TOKEN:?}"
    popd # "${drivers_tools_dir}/.evergreen/auth_aws"
    expect_success "mongodb://localhost/?authMechanism=MONGODB-AWS"
  )
  exit
fi

if [[ "${TESTCASE}" == "EC2" ]]; then
  echo "===== Testing auth via EC2 task metadata ====="
  # Do necessary setup for EC2
  # Create user on $external db.
  pushd "${drivers_tools_dir}/.evergreen/auth_aws"
  # shellcheck source=/dev/null
  . aws_setup.sh ec2
  popd # "${drivers_tools_dir}/.evergreen/auth_aws"

  echo "Valid credentials via EC2 - should succeed"
  expect_success "mongodb://localhost/?authMechanism=MONGODB-AWS"
  exit
fi

if [[ "${TESTCASE}" == "ECS" ]]; then
  echo "===== Testing auth via ECS task metadata ====="
  [[ -d "${drivers_tools_dir}" ]]

  # Set up the target directory.
  ECS_SRC_DIR=${drivers_tools_dir}/.evergreen/auth_aws/src
  mkdir -p "${ECS_SRC_DIR}/.evergreen"
  # Move the test script to the correct location.
  cp "${script_dir}/run-mongodb-aws-ecs-test.sh" "${ECS_SRC_DIR}/.evergreen"
  # Move artifacts needed for test to $ECS_SRC_DIR
  cp "${mongoc_dir}/src/libmongoc/test-awsauth" "${ECS_SRC_DIR}/"

  # Run the test
  pushd "${drivers_tools_dir}/.evergreen/auth_aws"
  PROJECT_DIRECTORY="$ECS_SRC_DIR" MONGODB_BINARIES=${mongoc_dir}/mongodb/bin ./aws_setup.sh ecs
  popd # "${drivers_tools_dir}/.evergreen/auth_aws"
  exit
fi

if [[ "${TESTCASE}" == "ASSUME_ROLE_WITH_WEB_IDENTITY" ]]; then
  echo "===== Testing auth via Web Identity ====="
  # Do necessary setup.
  # Create user on $external db.
  pushd "${drivers_tools_dir}/.evergreen/auth_aws"
  # shellcheck source=/dev/null
  . aws_setup.sh web-identity # Sets AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE
  : "${AWS_ROLE_ARN:?}" "${AWS_WEB_IDENTITY_TOKEN_FILE:?}"
  popd # "${drivers_tools_dir}/.evergreen/auth_aws"

  echo "Valid credentials via Web Identity - should succeed"
  AWS_ROLE_ARN="${AWS_ROLE_ARN}" \
  AWS_WEB_IDENTITY_TOKEN_FILE="${AWS_WEB_IDENTITY_TOKEN_FILE}" \
    expect_success "mongodb://localhost/?authMechanism=MONGODB-AWS"

  echo "Valid credentials via Web Identity with session name - should succeed"
  AWS_ROLE_ARN="${AWS_ROLE_ARN}" \
  AWS_WEB_IDENTITY_TOKEN_FILE="${AWS_WEB_IDENTITY_TOKEN_FILE}" \
  AWS_ROLE_SESSION_NAME=test \
    expect_success "mongodb://localhost/?authMechanism=MONGODB-AWS"

  echo "Invalid AWS_ROLE_ARN via Web Identity with session name - should fail"
  AWS_ROLE_ARN="invalid_role_arn" \
  AWS_WEB_IDENTITY_TOKEN_FILE="${AWS_WEB_IDENTITY_TOKEN_FILE}" \
    expect_failure "mongodb://localhost/?authMechanism=MONGODB-AWS"

  echo "Invalid AWS_WEB_IDENTITY_TOKEN_FILE via Web Identity with session name - should fail"
  AWS_ROLE_ARN="${AWS_ROLE_ARN}" \
  AWS_WEB_IDENTITY_TOKEN_FILE="/invalid/path" \
    expect_failure "mongodb://localhost/?authMechanism=MONGODB-AWS"

  echo "Invalid AWS_ROLE_SESSION_NAME via Web Identity with session name - should fail"
  AWS_ROLE_ARN="${AWS_ROLE_ARN}" \
  AWS_WEB_IDENTITY_TOKEN_FILE="${AWS_WEB_IDENTITY_TOKEN_FILE}" \
  AWS_ROLE_SESSION_NAME="contains_invalid_character_^" \
    expect_failure "mongodb://localhost/?authMechanism=MONGODB-AWS"
  exit
fi

echo "Unexpected testcase '${TESTCASE}'" 1>&2
exit 1
