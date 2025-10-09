#!/usr/bin/env bash

# Used by make_release.py.
# See: https://docs.devprod.prod.corp.mongodb.com/release-tools-container-images/garasign/garasign_signing/

set -o errexit
set -o pipefail

: "${1:?"missing dist_file as first argument"}"

# Allow customization point to use docker in place of podman.
launcher="${GARASIGN_LAUNCHER:-"podman"}"

if ! command -v "${launcher:?}" >/dev/null; then
  echo "${launcher:?} is required to sign distribution tarball" 1>&2
fi

if ! command -v gpg >/dev/null; then
  echo "gpg is required to verify distribution tarball signature" 1>&2
fi

artifactory_creds=~/.secrets/artifactory-creds.txt
garasign_creds=~/.secrets/garasign-creds.txt

unset ARTIFACTORY_USER ARTIFACTORY_PASSWORD
. "${artifactory_creds:?}"
: "${ARTIFACTORY_USER:?"missing ARTIFACTORY_USER in ${artifactory_creds:?}"}"
: "${ARTIFACTORY_PASSWORD:?"missing ARTIFACTORY_PASSWORD in ${artifactory_creds:?}"}"

unset GRS_CONFIG_USER1_USERNAME GRS_CONFIG_USER1_PASSWORD
. "${garasign_creds:?}"
: "${GRS_CONFIG_USER1_USERNAME:?"missing GRS_CONFIG_USER1_USERNAME in ${garasign_creds:?}"}"
: "${GRS_CONFIG_USER1_PASSWORD:?"missing GRS_CONFIG_USER1_PASSWORD in ${garasign_creds:?}"}"

dist_file="${1:?}"
dist_file_signed="${dist_file:?}.asc"

"${launcher:?}" login --password-stdin --username "${ARTIFACTORY_USER:?}" artifactory.corp.mongodb.com <<<"${ARTIFACTORY_PASSWORD:?}"

plugin_commands=(
  gpg --yes -v --armor -o "${dist_file_signed:?}" --detach-sign "${dist_file:?}"
)
"${launcher:?}" run \
  --env-file="${garasign_creds:?}" \
  -e "PLUGIN_COMMANDS=${plugin_commands[*]:?}" \
  --rm \
  -v "$(pwd):$(pwd)" \
  -w "$(pwd)" \
  artifactory.corp.mongodb.com/release-tools-container-registry-local/garasign-gpg

# Validate the signature file works as intended.
keyring="$(mktemp)"
curl -sS https://pgp.mongodb.com/cpp-driver.pub | gpg -q --no-default-keyring --keyring "${keyring:?}" --import -
gpgv --keyring "${keyring:?}" "${dist_file_signed:?}" "${dist_file:?}"
