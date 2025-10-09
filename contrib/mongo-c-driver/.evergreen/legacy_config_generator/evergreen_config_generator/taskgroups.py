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

from typing import MutableMapping
from evergreen_config_generator import ConfigObject

from . import Value, ValueSequence


class TaskGroup(ConfigObject):
    def __init__(self, name: str):
        self._task_group_name = name
        self.setup_group: ValueSequence | None = None
        self.teardown_group: ValueSequence | None = None
        self.setup_task: str | None = None
        self.teardown_task: str | None = None
        self.max_hosts: int | None = None
        self.timeout: int | None = None
        self.setup_group_can_fail_task: bool | None = None
        self.setup_group_timeout_secs: int | None = None
        self.share_processes: bool | None = None
        self.tasks: ValueSequence | None = None

    @property
    def name(self) -> str:
        return self._task_group_name

    def to_dict(self) -> Value:
        v = super().to_dict()
        assert isinstance(v, MutableMapping)
        # See possible TaskGroup attributes from the Evergreen wiki:
        # https://github.com/evergreen-ci/evergreen/wiki/Project-Configuration-Files#task-groups
        attrs = [
            "setup_group",
            "teardown_group",
            "setup_task",
            "teardown_task",
            "max_hosts",
            "timeout",
            "setup_group_can_fail_task",
            "setup_group_timeout_secs",
            "share_processes",
            "tasks",
        ]

        for i in attrs:
            if getattr(self, i, None):
                v[i] = getattr(self, i)
        return v
