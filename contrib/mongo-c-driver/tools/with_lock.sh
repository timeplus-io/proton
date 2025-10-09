#!/usr/bin/env bash

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" base

# * with-lock <lockfile> [command ...]
#       Execute ‘command’ while holding <lockfile> as an exclusive lock. This
#       requires the ‘lckdo’ command, otherwise it executes the command without
#       taking any lock. The parent directory of <lockfile> must exists. NOTE:
#       the given command must be an application, and not a shell-internal or
#       shell function.
with-lock() {
    [[ "$#" -gt 1 ]] || fail "with-lock requires a lock filename and a command to run"
    if ! have-command lckdo; then
        log "No ‘lckdo’ program is installed. We'll run without the lock, but parallel tasks may contend."
        log "  (‘lckdo’ is part of the ‘moreutils’ package)"
        shift
        command "$@"
    else
        lckdo -W 30 -- "$@"
    fi
}
