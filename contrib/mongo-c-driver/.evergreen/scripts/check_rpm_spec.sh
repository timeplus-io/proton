#!/usr/bin/env bash

#
# Copyright 2024 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o errexit

#
# check_rpm_spec.sh - Check if our RPM spec matches downstream's
#
# Supported/used environment variables:
#   IS_PATCH    If "true", this is an Evergreen patch build.


on_exit () {
  if [ -n "${SPEC_FILE}" ]; then
    rm -f "${SPEC_FILE}"
  fi
}
trap on_exit EXIT

if [ "${IS_PATCH}" = "true" ]; then
   echo "This is a patch build...skipping RPM spec check"
   exit
fi

SPEC_FILE=$(mktemp --tmpdir -u mongo-c-driver.XXXXXXXX.spec)
curl --retry 5 https://src.fedoraproject.org/rpms/mongo-c-driver/raw/rawhide/f/mongo-c-driver.spec -sS --max-time 120 --fail --output "${SPEC_FILE}"

diff -q .evergreen/etc/mongo-c-driver.spec "${SPEC_FILE}" || (echo "Synchronize RPM spec from downstream to fix this failure. See instructions here: https://docs.google.com/document/d/1ItyBC7VN383zNXu3oUOQJYR7adfYI8ECjLMJ5kqA9X8/edit#heading=h.ahdrr3b5xv3"; exit 1)
