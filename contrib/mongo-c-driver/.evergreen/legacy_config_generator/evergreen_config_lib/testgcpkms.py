#!/usr/bin/env python
#
# Copyright 2022 - present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import OrderedDict as OD
from typing import MutableSequence

from evergreen_config_generator.functions import shell_exec, func
from evergreen_config_generator.tasks import NamedTask
from evergreen_config_generator.variants import Variant
from evergreen_config_generator.taskgroups import TaskGroup


def _create_tasks():
    passtask = NamedTask(
        task_name="testgcpkms-task",
        commands=[
            func("fetch-source"),
            shell_exec(
                r"""
            echo "Building test-gcpkms ... begin"
            pushd mongoc
            ./.evergreen/scripts/compile-test-gcpkms.sh
            popd
            echo "Building test-gcpkms ... end"
            echo "Copying files ... begin"
            export GCPKMS_GCLOUD=${GCPKMS_GCLOUD}
            export GCPKMS_PROJECT=${GCPKMS_PROJECT}
            export GCPKMS_ZONE=${GCPKMS_ZONE}
            export GCPKMS_INSTANCENAME=${GCPKMS_INSTANCENAME}
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            mkdir testgcpkms
            cp ./mongoc/src/libmongoc/test-gcpkms ./mongoc/install/lib/libmongocrypt.* testgcpkms
            tar czf testgcpkms.tgz testgcpkms/*
            GCPKMS_SRC="testgcpkms.tgz" GCPKMS_DST=$GCPKMS_INSTANCENAME: $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/copy-file.sh
            echo "Copying files ... end"
            echo "Untarring file ... begin"
            GCPKMS_CMD="tar xf testgcpkms.tgz" $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/run-command.sh
            echo "Untarring file ... end"
            """,
                test=False,
                add_expansions_to_env=True,
            ),
            shell_exec(
                r"""
            export GCPKMS_GCLOUD=${GCPKMS_GCLOUD}
            export GCPKMS_PROJECT=${GCPKMS_PROJECT}
            export GCPKMS_ZONE=${GCPKMS_ZONE}
            export GCPKMS_INSTANCENAME=${GCPKMS_INSTANCENAME}
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            GCPKMS_CMD="LD_LIBRARY_PATH=./testgcpkms MONGODB_URI='mongodb://localhost:27017' ./testgcpkms/test-gcpkms" $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/run-command.sh
            """
            ),
        ],
    )

    failtask = NamedTask(
        task_name="testgcpkms-fail-task",
        commands=[
            shell_exec(
                r"""
            pushd mongoc
            ./.evergreen/scripts/compile-test-gcpkms.sh
            popd""",
                test=False,
                add_expansions_to_env=True,
            ),
            shell_exec(
                r"""
            export GCPKMS_GCLOUD=${GCPKMS_GCLOUD}
            export GCPKMS_PROJECT=${GCPKMS_PROJECT}
            export GCPKMS_ZONE=${GCPKMS_ZONE}
            export GCPKMS_INSTANCENAME=${GCPKMS_INSTANCENAME}
            LD_LIBRARY_PATH=$(pwd)/install MONGODB_URI='mongodb://localhost:27017' EXPECT_ERROR='Failed to connect to: metadata.google.internal' ./mongoc/src/libmongoc/test-gcpkms"""
            ),
        ],
    )

    return [passtask, failtask]


def _create_variant():
    return Variant(
        name="testgcpkms-variant",
        display_name="GCP KMS",
        # GCP Virtual Machine created is Debian 11.
        run_on="debian11-small",
        tasks=["testgcpkms_task_group", "testgcpkms-fail-task"],
        batchtime=20160,
    )  # Use a batchtime of 14 days as suggested by the CSFLE test README


def _create_task_group():
    task_group = TaskGroup(name="testgcpkms_task_group")
    task_group.setup_group_can_fail_task = True
    task_group.setup_group_timeout_secs = 1800  # 30 minutes
    task_group.setup_group = [
        func("fetch-det"),
        # Create and set up a GCE instance using driver tools script
        shell_exec(
            r"""
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            echo '${testgcpkms_key_file}' > /tmp/testgcpkms_key_file.json
            export GCPKMS_KEYFILE=/tmp/testgcpkms_key_file.json
            export GCPKMS_DRIVERS_TOOLS=$DRIVERS_TOOLS
            export GCPKMS_SERVICEACCOUNT="${testgcpkms_service_account}"
            $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/create-and-setup-instance.sh""",
            test=False,
        ),
        # Load the GCPKMS_GCLOUD, GCPKMS_INSTANCE, GCPKMS_PROJECT, and GCPKMS_ZONE expansions.
        OD([("command", "expansions.update"), ("params", OD([("file", "testgcpkms-expansions.yml")]))]),
    ]

    task_group.teardown_group = [
        shell_exec(
            r"""
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            export GCPKMS_GCLOUD=${GCPKMS_GCLOUD}
            export GCPKMS_PROJECT=${GCPKMS_PROJECT}
            export GCPKMS_ZONE=${GCPKMS_ZONE}
            export GCPKMS_INSTANCENAME=${GCPKMS_INSTANCENAME}
            $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/delete-instance.sh""",
            test=False,
        )
    ]
    task_group.tasks = ["testgcpkms-task"]
    return task_group


def testgcpkms_generate(
    all_tasks: MutableSequence[NamedTask],
    all_variants: MutableSequence[Variant],
    all_task_groups: MutableSequence[TaskGroup],
):
    all_tasks.extend(_create_tasks())
    all_variants.append(_create_variant())
    all_task_groups.append(_create_task_group())
