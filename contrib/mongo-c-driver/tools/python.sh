#!/usr/bin/env bash

## Commands defined by this file:
#
# * run-python [...]
#     • Execute the Python interpreter with the given arguments
# * find-python
#     • Searches for a Python executable that supports Python 3.8 or newer
#
# Run this file directly to invoke the same Python executable.
# Invoke this script with "--where" to print the path to the Python executable
# that would be used.

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" base

find-python() {
    pys=(
        py
        python3.14
        python3.13
        python3.12
        python3.11
        python3.10
        python3.9
        python3.8
        python3
        python
    )
    for cand in "${pys[@]}"; do
        # Find a Python that supports "x := 0", which was added in Python 3.8
        if have-command "$cand" && "$cand" -c "(x:=0)" > /dev/null 2>&1; then
            _found=$(type -P "$cand")
            break
        fi
    done
    if ! is-set _found; then
        fail "No Python (≥3.8) executable was found"
    fi

    debug "Found Python: $_found"
    printf %s "$_found"
}

run-python() {
    local py
    py=$(find-python)
    "$py" "$@"
}

if is-main; then
    if [[ "$*" = "--where" ]]; then
        printf "%s\n" "$(find-python)"
    else
        run-python "$@"
    fi
fi
