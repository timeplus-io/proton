#!/usr/bin/env bash

# Usage:
#   print_var X
print_var() {
  printf "%s: %s\n" "${1:?"print_var requires a variable to print"}" "${!1:-}"
}

# Usage:
#   check_var_opt X
#   check_var_opt X "default value"
check_var_opt() {
  : "${1:?"check_var_opt requires a variable to check"}"
  printf -v "${1}" "%s" "${!1:-"${2:-}"}"
  print_var "${1}"
}

# Usage:
#   check_var_req X
check_var_req() {
  : "${1:?"check_var_req requires a variable to check"}"
  : "${!1:?"required variable ${1} is unset or null!"}"
  print_var "${1}"
}
