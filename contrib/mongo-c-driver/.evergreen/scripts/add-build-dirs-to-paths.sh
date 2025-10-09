#!/usr/bin/env bash

case "${OSTYPE}" in
cygwin)
  export PATH
  PATH="$(pwd)/install-dir/bin:${PATH:-}"
  PATH="$(pwd)/src/libbson/Release:${PATH}"
  PATH="$(pwd)/src/libbson/Debug:${PATH}"
  chmod -f +x src/libmongoc/Debug/* || true
  chmod -f +x src/libbson/Debug/* || true
  chmod -f +x src/libmongoc/Release/* || true
  chmod -f +x src/libbson/Release/* || true
  chmod -f +x install-dir/bin/* || true
  ;;

darwin*)
  export DYLD_LIBRARY_PATH
  DYLD_LIBRARY_PATH="${EXTRA_LIB_PATH:-}:${DYLD_LIBRARY_PATH:-}"
  DYLD_LIBRARY_PATH="$(pwd)/src/libmongoc:${DYLD_LIBRARY_PATH}"
  DYLD_LIBRARY_PATH="$(pwd)/src/libbson:${DYLD_LIBRARY_PATH}"
  DYLD_LIBRARY_PATH="$(pwd)/install-dir/lib64:${DYLD_LIBRARY_PATH}"
  DYLD_LIBRARY_PATH="$(pwd)/install-dir/lib:${DYLD_LIBRARY_PATH}"
  DYLD_LIBRARY_PATH="$(pwd):${DYLD_LIBRARY_PATH}"
  ;;

*)
  export LD_LIBRARY_PATH
  LD_LIBRARY_PATH="${EXTRA_LIB_PATH:-}:${LD_LIBRARY_PATH:-}"
  LD_LIBRARY_PATH="$(pwd)/src/libmongoc:${LD_LIBRARY_PATH}"
  LD_LIBRARY_PATH="$(pwd)/src/libbson:${LD_LIBRARY_PATH}"
  LD_LIBRARY_PATH="$(pwd)/install-dir/lib64:${LD_LIBRARY_PATH}"
  LD_LIBRARY_PATH="$(pwd)/install-dir/lib:${LD_LIBRARY_PATH}"
  LD_LIBRARY_PATH="$(pwd):${LD_LIBRARY_PATH}"
  ;;
esac
