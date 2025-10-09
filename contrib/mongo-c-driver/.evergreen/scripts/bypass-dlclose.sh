#!/usr/bin/env bash

# bypass_dlclose
#
# Usage:
#   bypass_dlclose command args...
#
# Parameters:
#   "$CC": compiler to use to compile and link the bypass_dlclose library.
#
# Return 0 (true) if able to create a shared library to bypass calls to dlclose.
# Return a non-zero (false) value otherwise.
#
# If successful, print paths to add to LD_PRELOAD to stdout (pipe 1).
# Otherwise, no output is printed to stdout (pipe 1).
#
# Diagnostic messages may be printed to stderr (pipe 2). Redirect to /dev/null
# with `2>/dev/null` to silence these messages.
bypass_dlclose() (
  : "${CC:?'bypass_dlclose expects environment variable CC to be defined!'}"

  declare ld_preload

  {
    declare tmp

    if ! tmp="$(mktemp -d)"; then
      echo "Could not create temporary directory for bypass_dlclose library!" 1>&2
      return 1
    fi

    echo "int dlclose (void *handle) {(void) handle; return 0; }" \
      >|"${tmp}/bypass_dlclose.c" || return

    "${CC}" -o "${tmp}/bypass_dlclose.so" \
      -shared "${tmp}/bypass_dlclose.c" || return

    ld_preload="${tmp}/bypass_dlclose.so"

    # Clang uses its own libasan.so; do not preload it!
    if [ "${CC}" != "clang" ]; then
      declare asan_path
      asan_path="$(${CC} -print-file-name=libasan.so)" || return
      ld_preload="${asan_path}:${ld_preload}"
    fi
  } 1>&2

  printf "%s" "${ld_preload}"
)
