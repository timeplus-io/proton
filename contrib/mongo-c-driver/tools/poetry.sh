#!/usr/bin/env bash

# Automatically installs and invokes a pinned version of Poetry (https://python-poetry.org/)
#
## Commands defined by this file:
#
# * run-poetry [<cmd> ...]
#     • Execute the given Poetry command. This script can also be executed
#       directly to run the same Poetry command.
#
#       On first run, will install a new Poetry instance in a user-local cache
#       directory. This script DOES NOT respect POETRY_HOME! Instead, use
#       FORCE_POETRY_HOME to force a directory in which to install Poetry.
#
# * ensure-poetry [<version> [<home>]]
#     • Ensures that Poetry of the given <version> is installed into <home>.
#       This is an idempotent operation. Defaults are from WANT_POETRY_VERSION
#       and POETRY_HOME (see below).
#
## Variables set by this file:
#
# * POETRY_HOME
#     • The default user-local directory in which Poetry will be installed.
#       This can be overriden by setting FORCE_POETRY_HOME.
# * POETRY_EXE
#     • The default full path to the Poetry that will be installed and run by
#       this script (not present until after ensure-poetry or run-poetry is
#       executed).
# * WANT_POETRY_VERSION (overridable) (default 1.5.1)
#     • The version of Poetry that will be installed by run-poetry when executed.
# * POETRY_PYTHON_VERSION (overridable) (default to result of find-python)
#     • The Python binary to use by the Poetry installer and virtual environment(s).

# Load vars and utils:
. "$(dirname "${BASH_SOURCE[0]}")/use.sh" python paths base with_lock download

: "${WANT_POETRY_VERSION:=1.5.1}"
: "${POETRY_PYTHON_BINARY:="$(find-python)"}"
declare -r -x POETRY_HOME=${FORCE_POETRY_HOME:-"$BUILD_CACHE_DIR/poetry-$WANT_POETRY_VERSION"}
declare -r POETRY_EXE=$POETRY_HOME/bin/poetry$EXE_SUFFIX

# Usage: install-poetry <version> <poetry-home>
install-poetry() {
    declare poetry_version=$1
    declare poetry_home=$2
    log "Installing Poetry $poetry_version into [$poetry_home]"
    mkdir -p "$poetry_home"
    # Download the automated installer:
    installer=$poetry_home/install-poetry.py
    download-file --uri=https://install.python-poetry.org --out="$installer"
    # Run the install:
    with-lock "$POETRY_HOME/.install.lock" \
        env POETRY_HOME="$poetry_home" \
        "$POETRY_PYTHON_BINARY" -u "$installer" --yes --version "$poetry_version" \
    || (
        cat -- poetry-installer*.log && fail "Poetry installation failed"
    )
    printf %s "$poetry_version" > "$POETRY_HOME/installed.txt"
}

# Idempotent installation:
# Usage: ensure-poetry <version> <poetry-home>
ensure-poetry() {
    declare version=${1:-$WANT_POETRY_VERSION}
    declare home=${2:-$POETRY_HOME}
    if ! is-file "$home/installed.txt" || [[ "$(cat "$home/installed.txt")" != "$version" ]]; then
        install-poetry "$version" "$home"
    fi
    # Extra step must be taken to ensure Poetry's virtual environment uses the correct Python binary.
    # See: https://github.com/python-poetry/poetry/issues/522
    with-lock "$POETRY_HOME/.install.lock" \
        env POETRY_HOME="$POETRY_HOME" \
        "$POETRY_EXE" env use --quiet -- "$POETRY_PYTHON_BINARY" \
    || (
        fail "Poetry failed to set Python binary to $POETRY_PYTHON_BINARY"
    )
}

run-poetry() {
    ensure-poetry "$WANT_POETRY_VERSION" "$POETRY_HOME"
    env POETRY_HOME="$POETRY_HOME" "$POETRY_EXE" "$@"
}

# Poetry bug: Poetry uses the keyring, even for non-authenticating commands,
# which can wreak havoc in cases where the keyring is unavailable
# (i.e. SSH and non-interactive sessions)
# (https://github.com/python-poetry/poetry/issues/1917)
export PYTHON_KEYRING_BACKEND="keyring.backends.null.Keyring"

if is-main; then
    if [[ "$*" = "--ensure-installed" ]]; then
        # Just install, don't run it
        ensure-poetry "$WANT_POETRY_VERSION" "$POETRY_HOME"
    else
        # Run the Poetry command:
        run-poetry "$@"
    fi
fi
