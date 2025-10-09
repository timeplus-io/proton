#!/usr/bin/env bash

find_cmake_latest() {
  . "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths || return

  declare script_dir
  script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")" || return

  # shellcheck source=.evergreen/scripts/find-cmake-version.sh
  . "${script_dir}/find-cmake-version.sh" || return

  find_cmake_version 3 29 0
}
