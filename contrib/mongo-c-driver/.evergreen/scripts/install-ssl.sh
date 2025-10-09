#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_opt SSL "no"

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare install_dir="${mongoc_dir}/install-dir"

# Install OpenSSL library to separate install directory from normal
# "install-ssl" directory to avoid interfering with OpenSSL requirements by
# system binaries when install-dir/bin is prepended to $PATH.
declare openssl_install_dir="${mongoc_dir}/openssl-install-dir"

declare -a ssl_extra_flags

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi

# OpenSSL prior to 1.1.0 complains about "jobserver unavailable" if explicit N
# is given to `-j` then defaults to N=1. Prefer unbounded parallelism over
# none instead.
declare njobs
njobs="$(nproc)"

build_target_if_exists() {
  if make -n "${1:?}" 2>/dev/null; then
    make -s "${@}"
  fi
}

install_openssl() {
  declare ssl_version="${SSL##openssl-}"
  declare tmp
  tmp="$(echo "${ssl_version:?}" | tr . _)"
  curl -L --retry 5 -o ssl.tar.gz "https://github.com/openssl/openssl/archive/OpenSSL_${tmp}.tar.gz"
  tar zxf ssl.tar.gz
  pushd "openssl-OpenSSL_${tmp}"
  (
    set -o xtrace
    ./config --prefix="${openssl_install_dir}" "${ssl_extra_flags[@]}" shared -fPIC
    make -j depend
    build_target_if_exists "build_crypto"       # <1.1.0; parallel is broken.
    build_target_if_exists "build_engines" "-j" # <1.1.0
    build_target_if_exists "build_ssl" "-j"     # <1.1.0
    build_target_if_exists "build_libs" "-j"    # <1.1.0
    make -j
    make install_sw
  ) >/dev/null
  popd # "openssl-OpenSSL_${tmp}"
}

install_openssl_fips() {
  curl --retry 5 -o fips.tar.gz https://www.openssl.org/source/openssl-fips-2.0.16.tar.gz
  tar zxf fips.tar.gz
  pushd openssl-fips-2.0.16
  (
    set -o xtrace
    ./config --prefix="${openssl_install_dir}" -fPIC
    make -j build_crypto
    make build_fips # Parallel is broken.
    make install_sw
  ) >/dev/null
  popd # openssl-fips-2.0.16
  ssl_extra_flags=("--openssldir=${openssl_install_dir}" "--with-fipsdir=${openssl_install_dir}" "fips")
  SSL="${SSL%-fips}"
  install_openssl
}

install_libressl() {
  curl --retry 5 -o ssl.tar.gz "https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/${SSL}.tar.gz"
  tar zxf ssl.tar.gz
  pushd "${SSL}"
  (
    set -o xtrace
    ./configure --prefix="${install_dir}"
    make -s -j "${njobs}" install
  ) >/dev/null
  popd # "${SSL}"
}

case "${SSL}" in
openssl-*-fips)
  export LC_ALL
  LC_ALL="C" # Silence perl locale warnings.
  install_openssl_fips
  ;;

openssl-*)
  export LC_ALL
  LC_ALL="C" # Silence perl locale warnings.
  install_openssl
  ;;

libressl-*)
  install_libressl
  ;;
esac
