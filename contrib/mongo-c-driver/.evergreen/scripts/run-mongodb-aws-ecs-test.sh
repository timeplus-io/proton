#!/usr/bin/env bash

# run-mongodb-aws-ecs-test.sh is intended to run on a remote ECS host.
# For a description of ECS tests, see: https://github.com/mongodb-labs/drivers-evergreen-tools/tree/b01493d57e6716cb6385afaa4dc06330e4f33e01/.evergreen/auth_aws#ecs-test-process

# ECS tests have paths /root/src

echo "run-mongodb-aws-ecs-test.sh"

expect_success() {
  echo "Should succeed:"
  /root/src/test-awsauth "${1:?}" "EXPECT_SUCCESS"
}

expect_success "${1:?}"
