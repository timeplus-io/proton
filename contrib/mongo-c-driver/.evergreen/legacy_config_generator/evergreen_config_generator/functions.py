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
from textwrap import dedent
from typing import Iterable

from evergreen_config_generator import ConfigObject

from . import Value, MutableValueMapping, ValueMapping, ValueOrderedDict


def func(func_name: str, **kwargs: Value) -> MutableValueMapping:
    od: MutableValueMapping = OD([("func", func_name)])
    if kwargs:
        od["vars"] = OD(sorted(kwargs.items()))

    return od


def s3_put(remote_file: str, project_path: bool = True, **kwargs: Value) -> ValueMapping:
    if project_path:
        remote_file = "${project}/" + remote_file

    return ValueOrderedDict(
        [
            ("command", "s3.put"),
            (
                "params",
                ValueOrderedDict(
                    (
                        ("aws_key", "${aws_key}"),
                        ("aws_secret", "${aws_secret}"),
                        ("remote_file", remote_file),
                        ("bucket", "mciuploads"),
                        ("permissions", "public-read"),
                        *kwargs.items(),
                    )
                ),
            ),
        ]
    )


def strip_lines(s: str) -> str:
    return "\n".join(line for line in s.split("\n") if line.strip())


def shell_exec(
    script: str,
    test: bool = True,
    errexit: bool = True,
    xtrace: bool = False,
    silent: bool = False,
    continue_on_err: bool = False,
    working_dir: str | None = None,
    background: bool = False,
    add_expansions_to_env: bool = False,
    redirect_standard_error_to_output: bool = False,
    include_expansions_in_env: Iterable[str] = (),
) -> ValueMapping:
    dedented = ""
    if errexit:
        dedented += "set -o errexit\n"

    if xtrace:
        dedented += "set -o xtrace\n"

    dedented += dedent(strip_lines(script))
    command = ValueOrderedDict([("command", "shell.exec")])
    if test:
        command["type"] = "test"

    command["params"] = OD()
    if silent:
        command["params"]["silent"] = True

    if working_dir is not None:
        command["params"]["working_dir"] = working_dir

    if continue_on_err:
        command["params"]["continue_on_err"] = True

    if background:
        command["params"]["background"] = True

    if add_expansions_to_env:
        command["params"]["add_expansions_to_env"] = True

    if redirect_standard_error_to_output:
        command["params"]["redirect_standard_error_to_output"] = True

    if include_expansions_in_env:
        command["params"]["include_expansions_in_env"] = list(include_expansions_in_env)

    command["params"]["shell"] = "bash"
    command["params"]["script"] = dedented
    return command


def targz_pack(target: str, source_dir: str, *include: str) -> ValueMapping:
    return OD(
        [
            ("command", "archive.targz_pack"),
            ("params", OD([("target", target), ("source_dir", source_dir), ("include", list(include))])),
        ]
    )


class Function(ConfigObject):
    def __init__(self, *commands: Value):
        super(Function, self).__init__()
        self.commands = commands

    def to_dict(self) -> Value:
        return list(self.commands)
