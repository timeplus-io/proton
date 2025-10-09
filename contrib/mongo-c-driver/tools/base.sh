#!/usr/bin/env bash

## Commands defined by this file:
#
# * log [...]
#     • Print the given messages to stderr
# * debug [...]
#     • Print the given messages to stderr only if PRINT_DEBUG_LOGS is truthy
# * fail [...]
#     • Print the given messages to stderr and return non-zero
# * is-set <var>
#     • Return zero if the given argument names a variable that is defined
#       (empty strings are still considered to be defined)
# * run-chdir <dir> [<command> ...]
#     • Enter directory <dir> and execute the given command. The working
#       directory of the caller is not changed.
# * have-command <command>
#     • Return zero if <command> names a command that can be executed by the shell
# * is-file <path>
# * is-dir <path>
# * exists <path>
#     • Return zero if <path> names a file, directory, or either, respectively.


set -o errexit
set -o pipefail
set -o nounset

is-set() {
    [[ -n ${!1+x} ]]
}

log() {
    echo "${@}" 1>&2
    return 0
}

debug() {
    if [[ "${PRINT_DEBUG_LOGS:-0}" != "0" ]]; then
        log "${@}"
    fi
}

fail() {
    log "${@}"
    return 1
}

run-chdir() {
    [[ "$#" -gt 1 ]] || fail "run-chdir expects at least two arguments"
    local _dir="$1"
    shift
    pushd "$_dir" > /dev/null
    debug "Run in directory [$_dir]:" "$@"
    "$@"
    local _rc=$?
    popd > /dev/null
    return $_rc
}

is-file() { [[ -f "$1" ]];}
is-dir() { [[ -d "$1" ]];}
exists() { [[ -e "$1" ]];}

have-command() {
    [[ "$#" -eq 1 ]] || fail "have-command expects a single argument"
    type "$1" > /dev/null 2>&1
}

# Inhibit msys path conversion
export MSYS2_ARG_CONV_EXCL="*"
