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

from typing import Iterable
from evergreen_config_generator.functions import shell_exec


def shell_mongoc(
    script: str,
    test: bool = True,
    errexit: bool = True,
    xtrace: bool = False,
    silent: bool = False,
    continue_on_err: bool = False,
    background: bool = False,
    add_expansions_to_env: bool = False,
    redirect_standard_error_to_output: bool = False,
    include_expansions_in_env: Iterable[str] = (),
):
    return shell_exec(
        script,
        working_dir="mongoc",
        test=test,
        errexit=errexit,
        xtrace=xtrace,
        silent=silent,
        continue_on_err=continue_on_err,
        background=background,
        add_expansions_to_env=add_expansions_to_env,
        include_expansions_in_env=include_expansions_in_env,
        redirect_standard_error_to_output=redirect_standard_error_to_output,
    )
