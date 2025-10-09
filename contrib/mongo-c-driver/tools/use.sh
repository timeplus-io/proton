#!/usr/bin/env bash

: <<EOF
Source this script to import components into other scripts. Usage:

    source <path-to-use.sh> <name> [<name> [...]]

This script will enable strict mode in the parent. <name> refers to the filepath
stems of scripts that are sibling to use.sh (i.e. within the same directory, the
name of "foo.sh" is "foo").

Commands defined by importing this file:

  is-main
    • This command takes no arguments, and returns zero if the invoked in context
      where imports are not being resolved (i.e. the script is being executed
      directly rather than being imported)

EOF

set -o errexit
set -o pipefail
set -o nounset

# A utility that exits true if invoked outside of an importing context:
is-main() { $_IS_MAIN; }

# Grab the absolute path to the directory of this script:
pushd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null
_this_dir=$(pwd)
popd >/dev/null

# Keep a stack, of scripts being imported:
declare -a _USE_IMPORTING

# Inform scripts that they are being imported, not executed directly:
_IS_MAIN=false

for item in "$@"; do
    # Don't double-import items:
    _varname="_IMPORTED_$item"
    if [[ -n "${!_varname+n}" ]]; then
        continue
    fi
    # Push this item:
    _USE_IMPORTING+=("$item")
    # The file to be imported:
    file=$_this_dir/$item.sh
    ! [[ ${PRINT_DEBUG_LOGS:-} = 1 ]] || echo "Import: [$item]" 1>&2
    _err=0
    # Detect self-import:
    if printf '%s\0' "${BASH_SOURCE[@]}" | grep -qFxZ -- "$file"; then
        echo "File '$file' imports itself transitively" 1>&2
        _err=1
    fi
    # Detect non-existing imports:
    if ! [[ -f $file ]]; then
        echo "No script '$file' exists to import." 1>&2
        _err=1
    fi
    # Print the stacktrace of imports upon error:
    if [[ $_err -eq 1 ]]; then
        printf " • [%s] loaded by:\n" "${BASH_SOURCE[@]}" 1>&2
        echo " • (user)" 1>&2
        echo "Bailing out" 1>&2
        return 1
    fi
    # shellcheck disable=1090
    . "$file"
    # Recover item from the stack, since we may have recursed:
    item="${_USE_IMPORTING[${#_USE_IMPORTING[@]}-1]}"
    # Pop the top stack item:
    unset "_USE_IMPORTING[${#_USE_IMPORTING[@]}-1]"
    # Declare that the item has been imported, for future reference:
    declare "_IMPORTED_$item=1"
    ! [[ ${PRINT_DEBUG_LOGS:-} = 1 ]] || echo "Import: [$item] - done" 1>&2
done

# Set _IS_MAIN to zero if the import stack is empty
if [[ "${_USE_IMPORTING+${_USE_IMPORTING[*]}}" = "" ]]; then
    _IS_MAIN=true
fi
