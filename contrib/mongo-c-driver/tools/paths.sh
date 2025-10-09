#!/usr/bin/env bash

## Commands defined by this file:
#
# * to_absolute <path>
#     • Convert a given path into an absolute path. Relative paths are
#       resolved relative to the working directory. Removes redundant directory
#       separators.
# * native_path <path>
#     • On MinGW/Cygwin/MSYS, convert the given Cygwin path to a Windows-native
#       path. On other platforms, the argument is unmodified
#
## Variables set by this file:
#
# * TOOLS_DIR
#   • The path to the directory containing this script file
# * MONGOC_DIR
#   • The path to the top-level project directory
# * USER_CACHES_DIR (overridable)
#   • A user-local directory in which to store cached data
# * BUILD_CACHE_DIR (overridable)
#   • A user-local directory for caches related to these scripts
# * EXE_SUFFIX
#   • Expands to “.exe” on Windows, otherwise an empty string

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" platform base

# Check for Cygpath, used by various commands. Better to check once than check every time.
_HAVE_CYGPATH=false
if have-command cygpath; then
    _HAVE_CYGPATH=true
fi

# Usage: native-path <path>
native-path() {
    [[ "$#" -eq 1 ]] || fail "native_path expects exactly one argument"
    local arg=$1
    if $IS_WINDOWS; then
        $_HAVE_CYGPATH || fail "No 'cygpath' command is available, but we require it to normalize file paths on Windows."
        local ret
        ret="$(cygpath -w "$arg")"
        debug "Convert path [$arg] → [$ret]"
        printf %s "$ret"
    else
        printf %s "$arg"
    fi
}

# Usage: to_absolute <path>
to_absolute() {
    [[ "$#" -eq 1 ]] || fail "to_absolute expects a single argument"
    local ret
    local arg="$1"
    debug "Resolve path [$arg]"

    # Cygpath can resolve the path in a single subprocess:
    if $_HAVE_CYGPATH; then
        # Ask Cygpath to resolve the path. It knows how to do it reliably and quickly:
        ret=$(cygpath --absolute --mixed --long-name -- "$arg")
        debug "Cygpath resolved: [$arg]"
        printf %s "$ret"
        return 0
    fi

    # If the given directory exists, we can ask the shell to resolve the path
    # by going there and asking the PWD:
    if is-dir "$arg"; then
        ret=$(run-chdir "$arg" pwd)
        debug "Resolved: [$arg]"
        printf %s "$ret"
        return 0
    fi

    # Do it the "slow" way:

    # The parent path:
    local _parent
    _parent="$(dirname "$arg")"
    # The filename part:
    local _fname
    _fname="$(basename "$arg")"
    # There are four cases to consider from dirname:
    if [[ $_parent = "." ]]; then  # The parent is '.' as in './foo'
        # Replace the leading '.' with the working directory
        _parent="$(pwd)"
    elif [[ $_parent = ".." ]]; then  # The parent is '..' as in '../foo'
        # Replace a leading '..' with the parent of the working directory
        _parent="$(dirname "$(pwd)")"
    elif [[ $arg == "$_parent" ]]; then  # The parent is itself, as in '/'
        # A root directory is its own parent according to 'dirname'
        printf %s "$arg"
        return 0
    else  # The parent is some other path, like 'foo' in 'foo/bar'
        # Resolve the parent path
        _parent="$(set +x; DEBUG=0 to_absolute "$_parent")"
    fi
    # At this point $_parent is an absolute path
    if [[ $_fname = ".." ]]; then
        # Strip one component
        ret="$(dirname "$_parent")"
    elif [[ $_fname = "." ]]; then
        # Drop a '.' at the end of a path
        ret="$_parent"
    else
        # Join the result
        ret="$_parent/$_fname"
    fi
    # Remove duplicate dir separators
    while [[ $ret =~ "//" ]]; do
        ret="${ret//\/\///}"
    done
    debug "Resolved path: [$arg] → [$ret]"
    printf %s "$ret"
}

# Get the TOOLS_DIR as a native absolute path. All other path vars are derived
# from this one, and will therefore remain as native pathsS
_this_file=$(to_absolute "${BASH_SOURCE[0]}")
_this_dir=$(dirname "$_this_file")
TOOLS_DIR=$(native-path "$_this_dir")
declare -r TOOLS_DIR=$TOOLS_DIR

MONGOC_DIR=$(dirname "$TOOLS_DIR")
declare -r MONGOC_DIR=$MONGOC_DIR

EXE_SUFFIX=""
if $IS_WINDOWS; then
    EXE_SUFFIX=".exe"
fi
declare -r EXE_SUFFIX=$EXE_SUFFIX

if [[ "${USER_CACHES_DIR:=${XDG_CACHE_HOME:-}}" = "" ]]; then
    if $IS_DARWIN; then
        USER_CACHES_DIR=$HOME/Library/Caches
    elif $IS_UNIX_LIKE; then
        USER_CACHES_DIR=$HOME/.cache
    elif $IS_WINDOWS; then
        USER_CACHES_DIR=${LOCALAPPDATA:-$USERPROFILE/.cache}
    else
        log "Using ~/.cache as fallback user caching directory"
        USER_CACHES_DIR="$(to_absolute ~/.cache)"
    fi
fi

# Ensure we are dealing with a complete path
USER_CACHES_DIR="$(to_absolute "$USER_CACHES_DIR")"
declare -r USER_CACHES_DIR=$USER_CACHES_DIR

: "${BUILD_CACHE_BUST:=1}"
: "${BUILD_CACHE_DIR:="$USER_CACHES_DIR/mongoc/build.$BUILD_CACHE_BUST"}"

if is-main; then
    # Just print the paths that we detected
    log "Paths:"
    log " • USER_CACHES_DIR=[$USER_CACHES_DIR]"
    log " • BUILD_CACHE_DIR=[$BUILD_CACHE_DIR]"
    log " • BUILD_CACHE_BUST=[$BUILD_CACHE_BUST]"
    log " • EXE_SUFFIX=[$EXE_SUFFIX]"
    log " • TOOLS_DIR=[$TOOLS_DIR]"
    log " • MONGOC_DIR=[$MONGOC_DIR]"
fi
