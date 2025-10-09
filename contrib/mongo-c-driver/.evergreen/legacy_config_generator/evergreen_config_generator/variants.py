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

from typing import Iterable, Mapping
from evergreen_config_generator import ConfigObject

from . import ValueMapping


class Variant(ConfigObject):
    def __init__(
        self,
        name: str,
        display_name: str,
        run_on: list[str] | str,
        tasks: Iterable[str | ValueMapping],
        expansions: Mapping[str, str] | None = None,
        tags: Iterable[str] = (),
        batchtime: int | None = None,
        display_tasks: Iterable[ValueMapping] = None,
    ):
        super(Variant, self).__init__()
        self._variant_name = name
        self.display_name = display_name
        self.run_on = run_on
        self.tasks = tasks
        self.expansions = expansions
        self.tags = list(tags)
        self.batchtime = batchtime
        self.display_tasks = display_tasks

    @property
    def name(self):
        return self._variant_name

    def to_dict(self):
        v = super(Variant, self).to_dict()
        for i in "display_name", "expansions", "run_on", "tasks", "batchtime", "tags", "display_tasks":
            if getattr(self, i):
                v[i] = getattr(self, i)
        return v
