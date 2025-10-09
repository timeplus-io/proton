#!/usr/bin/env bash

## Commands defined by this file:
#
# * download-file --uri=<uri> --out=<filepath> [--no-tls-verify]
#     • Download the HTTP resource specified by <uri> and save the respond body
#       to <filepath>. Follows redirects.
#
#       If `--no-tls-verify` is provided, TLS certificate validation will be
#       disabled.

. "$(dirname "${BASH_SOURCE[0]}")/use.sh" base

download-file() {
    declare uri
    declare out
    unset no_tls_verify
    while [[ "$#" != 0 ]]; do
        case "$1" in
            --uri)
                shift
                uri=$1
                ;;
            --uri=*)
                uri="${1#--uri=*}"
                ;;
            --out)
                shift
                out=$1
                ;;
            --out=*)
                out=${1#--out=*}
                ;;
            --no-tls-verify)
                # shellcheck disable=2034
                no_tls_verify=1
                ;;
            *)
                fail "Unknown argument: $1"
        esac
        shift
    done
    if ! is-set uri || ! is-set out; then
        fail "download-file requires --uri=<uri> and --out=<filepath> arguments"
        return
    fi
    debug "Download [$uri] to [$out]"

    if have-command curl; then
        curl_argv=(
            --silent
            --show-error
            --fail
            --retry 5
            --max-time 120
            --location  # (Follow redirects)
            --output "$out"
        )
        if is-set no_tls_verify; then
            curl_argv+=(--insecure)
        fi
        curl_argv+=(-- "$uri")
        debug "Execute curl command: [curl ${curl_argv[*]}]"
        output=$(curl "${curl_argv[@]}") || fail "$output" || return
        debug "$output"
    elif have-command wget; then
        wget_argv=(
            --output-document="$out"
            --tries=5
            --timeout=120
        )
        if is-set no_tls_verify; then
            wget_argv+=(--no-check-certificate)
        fi
        wget_argv+=(-- "$uri")
        debug "Execute wget command: [wget ${wget_argv[*]}]"
        output=$(wget "${wget_argv[@]}" 2>&1) || fail "wget failed: $output" || return
        debug "$output"
    else
        fail "This script requires either curl or wget to be available" || return
    fi
    debug "Download [$uri] to [$out] - Done"
}


if is-main; then
    download-file "$@"
fi
