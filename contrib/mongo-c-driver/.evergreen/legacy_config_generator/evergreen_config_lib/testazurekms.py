#!/usr/bin/env python
#
# Copyright 2022-present MongoDB, Inc.
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


from collections import OrderedDict as OD
from typing import MutableSequence

from evergreen_config_generator.functions import shell_exec, func
from evergreen_config_generator.tasks import NamedTask
from evergreen_config_generator.variants import Variant
from evergreen_config_generator.taskgroups import TaskGroup


def _create_tasks():
    # passtask is expected to run on a remote Azure VM and succeed at obtaining credentials.
    passtask = NamedTask(task_name="testazurekms-task")
    passtask.commands = [
        func("fetch-source"),
        shell_exec(
            r"""
            echo "Building test-azurekms ... begin"
            pushd mongoc
            ./.evergreen/scripts/compile-test-azurekms.sh
            popd
            echo "Building test-azurekms ... end"

            echo "Copying files ... begin"
            export AZUREKMS_RESOURCEGROUP=${testazurekms_resourcegroup}
            export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
            export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            mkdir testazurekms
            cp ./mongoc/src/libmongoc/test-azurekms ./mongoc/install/lib/libmongocrypt.* testazurekms
            tar czf testazurekms.tgz testazurekms/*
            AZUREKMS_SRC="testazurekms.tgz" \
            AZUREKMS_DST="./" \
                $DRIVERS_TOOLS/.evergreen/csfle/azurekms/copy-file.sh
            echo "Copying files ... end"

            echo "Untarring file ... begin"
            AZUREKMS_CMD="tar xf testazurekms.tgz" \
                $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
            echo "Untarring file ... end"
            """,
            test=False,
            add_expansions_to_env=True,
        ),
        shell_exec(
            r"""
            export AZUREKMS_RESOURCEGROUP=${testazurekms_resourcegroup}
            export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
            export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            AZUREKMS_CMD="LD_LIBRARY_PATH=./testazurekms MONGODB_URI='mongodb://localhost:27017' KEY_NAME='${testazurekms_keyname}' KEY_VAULT_ENDPOINT='${testazurekms_keyvaultendpoint}' ./testazurekms/test-azurekms" \
                $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
            """
        ),
    ]

    failtask = NamedTask(task_name="testazurekms-fail-task")
    failtask.commands = [
        func("fetch-source"),
        shell_exec(
            r"""
            pushd mongoc
            ./.evergreen/scripts/compile-test-azurekms.sh
            popd
            """,
            test=False,
            add_expansions_to_env=True,
        ),
        shell_exec(
            r"""
            LD_LIBRARY_PATH=$PWD/install \
            MONGODB_URI='mongodb://localhost:27017' \
            KEY_NAME='${testazurekms_keyname}' \
            KEY_VAULT_ENDPOINT='${testazurekms_keyvaultendpoint}' \
            EXPECT_ERROR='Error from Azure IMDS server' \
                ./mongoc/src/libmongoc/test-azurekms
            """
        ),
    ]
    return [passtask, failtask]


def _create_variant():
    return Variant(
        name="testazurekms-variant",
        display_name="Azure KMS",
        # Azure Virtual Machine created is Debian 10.
        run_on="debian10-small",
        tasks=["testazurekms_task_group", "testazurekms-fail-task"],
        batchtime=20160,
    )  # Use a batchtime of 14 days as suggested by the CSFLE test README


def _create_task_group():
    task_group = TaskGroup(name="testazurekms_task_group")
    task_group.setup_group_can_fail_task = True
    task_group.setup_group_timeout_secs = 1800  # 30 minutes
    task_group.setup_group = [
        func("fetch-det"),
        shell_exec(
            r"""
            DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
            echo '${testazurekms_publickey}' > /tmp/testazurekms_publickey
            echo '${testazurekms_privatekey}' > /tmp/testazurekms_privatekey
            # Set 600 permissions on private key file. Otherwise ssh / scp may error with permissions "are too open".
            chmod 600 /tmp/testazurekms_privatekey
            export AZUREKMS_CLIENTID=${testazurekms_clientid}
            export AZUREKMS_TENANTID=${testazurekms_tenantid}
            export AZUREKMS_SECRET=${testazurekms_secret}
            export AZUREKMS_DRIVERS_TOOLS=$DRIVERS_TOOLS
            export AZUREKMS_RESOURCEGROUP=${testazurekms_resourcegroup}
            export AZUREKMS_PUBLICKEYPATH=/tmp/testazurekms_publickey
            export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
            export AZUREKMS_SCOPE=${testazurekms_scope}
            export AZUREKMS_VMNAME_PREFIX=CDRIVER
            $DRIVERS_TOOLS/.evergreen/csfle/azurekms/create-and-setup-vm.sh
            """,
            test=False,
        ),
        # Load the AZUREKMS_VMNAME expansion.
        OD(
            [
                ("command", "expansions.update"),
                (
                    "params",
                    OD(
                        [
                            ("file", "testazurekms-expansions.yml"),
                        ]
                    ),
                ),
            ]
        ),
    ]

    task_group.teardown_group = [
        # Load expansions again. The setup task may have failed before running `expansions.update`.
        OD(
            [
                ("command", "expansions.update"),
                (
                    "params",
                    OD(
                        [
                            ("file", "testazurekms-expansions.yml"),
                        ]
                    ),
                ),
            ]
        ),
        shell_exec(
            r"""
                DRIVERS_TOOLS=$(pwd)/drivers-evergreen-tools
                export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
                export AZUREKMS_RESOURCEGROUP=${testazurekms_resourcegroup}
                $DRIVERS_TOOLS/.evergreen/csfle/azurekms/delete-vm.sh
            """,
            test=False,
        ),
    ]
    task_group.tasks = ["testazurekms-task"]
    return task_group


def testazurekms_generate(
    all_tasks: MutableSequence[NamedTask],
    all_variants: MutableSequence[Variant],
    all_task_groups: MutableSequence[TaskGroup],
):
    all_tasks.extend(_create_tasks())
    all_variants.append(_create_variant())
    all_task_groups.append(_create_task_group())
