#!/usr/bin/env bash

if [[ "${OSTYPE}" == "cygwin" ]]; then
  bash .evergreen/scripts/compile-windows.sh
else
  bash .evergreen/scripts/compile-unix.sh
fi
