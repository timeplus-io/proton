#!/bin/sh

# This is a tiny shell utility for string manipulation.
# Allow us to use "local"
# shellcheck disable=SC3043

set -eu

_HELP='Usage:
  str {lower,upper}
  str test

Commands:
  lower, upper
    Convert input (stdin) to all-lowercase or all-uppercase, respectively

  test <str1> (-ieq|-ine|-contains|-matches) <str2>
    Like "test", but with additional string comparisons:
      -ieq • case-insensitive equal
      -ine • case-insensitive not-equal
      -contains • Check if <str1> contains <str2>
      -matches • Check if <str1> matches pattern <str2> (A grep -E pattern)
'

fail() {
  # shellcheck disable=SC2059
  printf -- "$@" 1>&2
  printf -- "\n" 1>&2
  return 1
}

__str__upper() {
  __justStdin upper __upper "$@"
}
__upper() {
  tr '[:lower:]' '[:upper:]'
}

__str__lower() {
  __justStdin lower __lower "$@"
}
__lower() {
  tr '[:upper:]' '[:lower:]'
}

__justStdin() {
  if test $# -gt 2; then
    fail "Command '%s' does not take any arguments (write input into stdin)" "$1" || return
  fi
  "$2"
}

__str__help() {
  printf %s "$_HELP"
}
__str____help() {
  __str help
}
__str___h() {
  __str help
}
__str___help() {
  __str help
}

__str__test() {
  test "$#" -eq 3 || fail '“str test” expects three arguments (Got %d: “%s”)' $# "$*" \
    || return
  local lhs="$1"
  local op="$2"
  local rhs="$3"
  local norm_lhs norm_rhs;
  norm_lhs=$(echo "$lhs" | __str lower) || return
  norm_rhs=$(echo "$rhs" | __str lower) || return
  case $op in
    -ieq)
      test "$norm_lhs" = "$norm_rhs";;
    -ine)
      test "$norm_lhs" != "$norm_rhs";;
    -matches)
      printf %s "$lhs" | grep -qE -- "$rhs";;
    -contains)
      printf %s "$lhs" | grep -qF -- "$rhs";;
    -*|=*)
      # Just defer to the underlying test command
      test "$lhs" "$op" "$rhs"
  esac
}

__str() {
  local _Command="$1"
  local _CommandIdent
  _CommandIdent="$(echo "__str__$_Command" | sed '
    s/-/_/g
    s/\./__/g
  ')"
  shift
  "$_CommandIdent" "$@"
}

__str "$@"
