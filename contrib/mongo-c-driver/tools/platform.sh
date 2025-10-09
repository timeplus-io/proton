#!/usr/bin/env bash

## Variables set by this file:
#
# * IS_DARWIN, IS_WINDOWS, IS_LINUX, IS_BSD, IS_WSL, IS_UNIX_LIKE
#   • Set to either "true" or "false" depending on the host operating system.
#     More than one value may be true (i.e. $IS_WSL and $IS_UNIX_LIKE). Because
#     "true" and "false" are Bash built-ins, these can be used in conditionals
#     directly, as in "if $IS_WINDOWS || $IS_DARWIN; then …"
# * OS_FAMILY
#   • One of "windows", "linux", "darwin", or "bsd", depending on the host
#     operating system.

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" base

_is_darwin=false
_is_windows=false
_is_linux=false
_is_unix_like=false
_is_wsl=false
_is_bsd=false
_os_family=unknown
case "$OSTYPE" in
    linux-*)
        if have-command cmd.exe; then
            _is_wsl=true
            _is_unix_like=true
            _os_family=windows
        else
            _is_linux=true
            _is_unix_like=true
            _os_family=linux
        fi
        ;;
    darwin*)
        _is_darwin=true
        _is_unix_like=true
        _os_family=darwin
        ;;
    FreeBSD|openbsd*|netbsd)
        _is_bsd=true
        _is_unix_like=true
        _os_family=bsd
        ;;
    msys*|cygwin*)
        _is_windows=true
        _os_family=windows
        ;;
esac

declare -r IS_DARWIN=$_is_darwin
declare -r IS_WINDOWS=$_is_windows
declare -r IS_LINUX=$_is_linux
declare -r IS_UNIX_LIKE=$_is_unix_like
declare -r IS_WSL=$_is_wsl
declare -r IS_BSD=$_is_bsd
declare -r OS_FAMILY=$_os_family

_is_redhat_based=false
_is_debian_based=false
if $IS_LINUX; then
    if is-file /etc/redhat-release; then
        _is_redhat_based=true
        _dist_version=$(sed 's|.*release \([^ ]\+\).*|\1|' < /etc/redhat-release)
    elif is-file /etc/debian_version; then
        _is_debian_based=true
        _dist_version=$(grep VERSION_ID /etc/os-release | sed 's|VERSION_ID="\(.*\)"|\1|')
    elif is-file /etc/alpine-release; then
        _is_alpine=true
        _dist_version=$(cat /etc/alpine-release)
    fi
    _dist_version=${_dist_version:-0}
    _major_version=${_dist_version/.*/}
    declare -r DIST_VERSION=$_dist_version
    declare -r DIST_MAJOR_VERSION=$_major_version

    if is-file /etc/redhat-release; then
        _dist_id=$(cut -d ' ' -f1 < /etc/redhat-release)
        declare -r DIST_ID=${_dist_id}
    elif is-file /etc/os-release; then
        _dist_id=$(grep '^ID=' /etc/os-release | sed 's|ID=||')
        declare -r DIST_ID=${_dist_id}
    fi
elif $IS_DARWIN; then
    _version=$(sw_vers | grep ProductVersion | sed 's|ProductVersion: \(.*\)|\1|')
    _major_version=${_version/.*/}
    declare -r MACOS_VERSION=${_version}
    declare -r MACOS_MAJOR_VERSION=${_major_version}
fi

declare -r IS_REDHAT_BASED=${_is_redhat_based}
declare -r IS_DEBIAN_BASED=${_is_debian_based}

_is_x86=false
_is_x64=false
_is_arm=false
_is_ppc=false
_is_zseries=false
_archname=""
case "$HOSTTYPE" in
    x86_64)
        _is_x86=true
        _is_x64=true
        _archname=x64
        ;;
    x86*)
        _is_x86=true
        _archname="x86"
        ;;
    aarch64|arm64)
        _is_arm=true
        _archname="arm64"
        ;;
    powerpc*)
        _is_ppc=true
        _archname="ppc"
        ;;
    s390x)
        _is_zseries=true
        _archname="s390x"
        ;;
    *)
        log "Unknown host architecture in HOSTTYPE '$HOSTTYPE'";;
esac
declare -r IS_X86=$_is_x86
declare -r IS_X64=$_is_x64
declare -r IS_ARM=$_is_arm
declare -r IS_POWERPC=$_is_ppc
declare -r IS_ZSERIES=$_is_zseries
declare -r ARCHNAME=$_archname

if is-set DIST_ID; then
    _os_shortname="${DIST_ID}${DIST_MAJOR_VERSION}"
elif $IS_WINDOWS; then
    _os_shortname=win
elif $IS_DARWIN; then
    _os_shortname=macos$MACOS_VERSION
else
    _os_shortname=unknown
fi
declare OS_SHORTNAME=$_os_shortname

if is-main; then
    log "Operating system detection:"
    log "  • OS_SHORTNAME: $OS_SHORTNAME"
    log "  • OS_FAMILY: $OS_FAMILY"
    log "  • IS_WINDOWS: $IS_WINDOWS"
    log "  • IS_DARWIN: $IS_DARWIN"
    log "  • IS_LINUX: $IS_LINUX"
    log "  • IS_BSD: $IS_BSD"
    log "  • IS_WSL: $IS_WSL"
    log "  • IS_UNIX_LIKE: $IS_UNIX_LIKE"
    log "  • IS_REDHAT_BASED: $IS_REDHAT_BASED"
    log "  • IS_DEBIAN_BASED: $IS_DEBIAN_BASED"
    log "  • DIST_ID: ${DIST_ID:-⟨unset⟩}"
    log "  • DIST_VERSION: ${DIST_VERSION:-⟨unset⟩}"
    log "  • DIST_MAJOR_VERSION: ${DIST_MAJOR_VERSION:-⟨unset⟩}"
    log "  • MACOS_VERSION: ${MACOS_VERSION:-⟨unset⟩}"
    log "  • MACOS_MAJOR_VERSION: ${MACOS_MAJOR_VERSION:-⟨unset⟩}"
    log "  • IS_X86: $IS_X86"
    log "  • IS_X64: $IS_X64"
    log "  • IS_ARM: $IS_ARM"
    log "  • IS_POWERPC: $IS_POWERPC"
    log "  • IS_ZSERIES: $IS_ZSERIES"
    log "  • ARCHNAME: $ARCHNAME"
fi
