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

from collections import OrderedDict as OD
import copy
from itertools import chain, product
import itertools
from typing import ClassVar, Iterable, Literal, Mapping, MutableMapping, Sequence, Union

from evergreen_config_generator import ConfigObject
from evergreen_config_generator.functions import func

from . import Value, MutableValueMapping, ValueSequence


DependencySpec = Union[str, Mapping[str, Value]]


class Task(ConfigObject):
    def __init__(
        self,
        task_name: str | None = None,
        commands: Iterable[Value] = (),
        tags: Iterable[str] = (),
        depends_on: Iterable[DependencySpec] = (),
        exec_timeout_secs: int | None = None,
    ):
        self._name = task_name
        self._tags = list(tags)
        self.options: MutableValueMapping = OD()
        self.commands: ValueSequence = list(commands)
        self.exec_timeout_secs = exec_timeout_secs
        self._depends_on = list(map(self._normal_dep, depends_on))

        if exec_timeout_secs is not None:
            self.options["exec_timeout_secs"] = exec_timeout_secs

    @property
    def dependencies(self) -> Sequence[Mapping[str, Value]]:
        main = list(self._depends_on)
        main.extend(map(self._normal_dep, self.additional_dependencies()))
        return tuple(main)

    def _normal_dep(self, spec: DependencySpec) -> Mapping[str, Value]:
        if isinstance(spec, str):
            return OD([("name", spec)])
        return spec

    @property
    def tags(self) -> Sequence[str]:
        return tuple(sorted(chain(self.additional_tags(), self._tags)))

    def pre_commands(self) -> Iterable[Value]:
        return ()

    def main_commands(self) -> Iterable[Value]:
        return ()

    def post_commands(self) -> Iterable[Value]:
        return ()

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        return ()

    @property
    def name(self) -> str:
        assert self._name is not None, f'Task {self} did not set a name, and did not override the "name" property'
        return self._name

    def additional_tags(self) -> Iterable[str]:
        return ()

    def add_dependency(self, dependency: DependencySpec):
        if isinstance(dependency, str):
            dependency = OD([("name", dependency)])

        self._depends_on.append(dependency)

    def to_dict(self):
        task: MutableValueMapping = super().to_dict()  # type: ignore
        assert isinstance(task, MutableMapping)
        if self.tags:
            task["tags"] = list(self.tags)
        task.update(self.options)
        deps: Sequence[MutableValueMapping] = list(self.dependencies)  # type: ignore
        if deps:
            if len(deps) == 1:
                task["depends_on"] = OD(deps[0])
            else:
                task["depends_on"] = copy.deepcopy(deps)
        task["commands"] = list(
            itertools.chain(
                self.pre_commands(),
                self.main_commands(),
                self.commands,
                self.post_commands(),
            )
        )
        return task


NamedTask = Task


class FuncTask(NamedTask):
    def __init__(
        self,
        task_name: str,
        functions: Iterable[str],
        tags: Iterable[str] = (),
        depends_on: Iterable[DependencySpec] = (),
        exec_timeout_secs: int | None = None,
    ):
        commands = [func(func_name) for func_name in functions]
        super().__init__(task_name, commands, tags=tags, depends_on=depends_on, exec_timeout_secs=exec_timeout_secs)
        super(FuncTask, self).__init__(task_name, commands=commands)


class Prohibited(Exception):
    pass


def require(rule: bool) -> None:
    if not rule:
        raise Prohibited()


def prohibit(rule: bool) -> None:
    if rule:
        raise Prohibited()


def both_or_neither(rule0: bool, rule1: bool) -> None:
    if rule0:
        require(rule1)
    else:
        prohibit(rule1)


class SettingsAccess:
    def __init__(self, inst: "MatrixTask") -> None:
        self._task = inst

    def __getattr__(self, __setting: str) -> str | bool:
        return self._task.setting_value(__setting)


class MatrixTask(Task):
    axes: ClassVar[Mapping[str, Sequence[str | bool]]] = OD()

    def __init__(self, settings: Mapping[str, str | bool]):
        super().__init__()
        self._settings = {k: v for k, v in settings.items()}
        for axis, options in type(self).axes.items():
            if axis not in self._settings:
                self._settings[axis] = options[0]

    def display(self, axis_name: str) -> str:
        value = self.setting_value(axis_name)
        if value is False:
            # E.g., if self.auth is False, return 'noauth'.
            return f"no{axis_name}"
        elif value is True:
            return axis_name
        else:
            return value

    def on_off(self, key: str, val: str) -> Literal["on", "off"]:
        return "on" if self.setting_value(key) == val else "off"

    @property
    def name(self) -> str:
        return "-".join(self.name_parts())

    def name_parts(self) -> Iterable[str]:
        raise NotImplementedError

    @property
    def settings(self) -> SettingsAccess:
        return SettingsAccess(self)

    def setting_value(self, axis: str) -> str | bool:
        assert (
            axis in type(self).axes.keys()
        ), f'Attempted to inspect setting "{axis}", which is not defined for this task type'
        return self._settings[axis]

    def setting_eq(self, axis: str, val: str | bool) -> bool:
        current = self.setting_value(axis)
        options = type(self).axes[axis]
        assert (
            val in options
        ), f'Looking for value "{val}" on setting "{axis}", but that is not a supported option (Expects one of {options})'
        return current == val

    def is_valid_combination(self) -> bool:
        try:
            return self.do_is_valid_combination()
        except Prohibited:
            print(f"Ignoring invalid combination {self.name!r}")
            return False

    def do_is_valid_combination(self) -> bool:
        return True

    @classmethod
    def matrix(cls):
        for cell in product(*cls.axes.values()):
            axis_values = dict(zip(cls.axes, cell))
            task = cls(settings=axis_values)
            if task.allowed:
                yield task

    @property
    def allowed(self):
        try:
            return self.do_is_valid_combination()
        except Prohibited:
            return False
