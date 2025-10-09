#!/usr/bin/env python
#
# Copyright 2018-present MongoDB, Inc.
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

"""Generate C Driver's config.yml for Evergreen testing.

We find that generating configuration from Python data structures is more
legible than Evergreen's matrix syntax or a handwritten file.

Requires the evergreen_config_generator package.
"""

from collections import OrderedDict as OD
from os.path import dirname, join as joinpath, normpath

from evergreen_config_generator import generate

from evergreen_config_lib.functions import all_functions
from evergreen_config_lib.tasks import all_tasks
from evergreen_config_lib.variants import all_variants
from evergreen_config_lib.taskgroups import all_task_groups
from evergreen_config_lib.testgcpkms import testgcpkms_generate
from evergreen_config_lib.testazurekms import testazurekms_generate

task_groups = list(all_task_groups)
testazurekms_generate(all_tasks, all_variants, task_groups)
testgcpkms_generate(all_tasks, all_variants, task_groups)

config = OD(
    [
        ("functions", all_functions),
        ("tasks", all_tasks),
        ("task_groups", task_groups),
        ("buildvariants", all_variants),
    ]
)

this_dir = dirname(__file__)
generated_configs_dir = normpath(joinpath(this_dir, "../generated_configs"))
generate(config, joinpath(generated_configs_dir, "legacy-config.yml"))
