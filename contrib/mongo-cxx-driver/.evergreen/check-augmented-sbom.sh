#!/usr/bin/env bash

set -o errexit
set -o pipefail

command -v podman >/dev/null || {
  echo "missing required program podman" 1>&2
  exit 1
}

command -v jq >/dev/null || {
  echo "missing required program jq" 1>&2
  exit 1
}

podman login --password-stdin --username "${ARTIFACTORY_USER:?}" artifactory.corp.mongodb.com <<<"${ARTIFACTORY_PASSWORD:?}"

# Ensure latest version of SilkBomb is being used.
podman pull artifactory.corp.mongodb.com/release-tools-container-registry-public-local/silkbomb:1.0

silkbomb_download_flags=(
  # Avoid bumping version or timestamp in diff.
  --no-update-sbom-version
  --no-update-timestamp

  --silk-asset-group mongo-cxx-driver
  -o /pwd/etc/augmented.sbom.json.new
)

podman run \
  --env-file <(
    echo "SILK_CLIENT_ID=${SILK_CLIENT_ID:?}"
    echo "SILK_CLIENT_SECRET=${SILK_CLIENT_SECRET:?}"
  ) \
  -it --rm -v "$(pwd):/pwd" \
  artifactory.corp.mongodb.com/release-tools-container-registry-public-local/silkbomb:1.0 \
  download "${silkbomb_download_flags[@]:?}"

[[ -f ./etc/augmented.sbom.json.new ]] || {
  echo "failed to download Augmented SBOM from Silk" 1>&2
  exit 1
}

echo "Comparing Augmented SBOM..."

jq -S '.' ./etc/augmented.sbom.json >|old.json
jq -S '.' ./etc/augmented.sbom.json.new >|new.json

# Allow task to upload the augmented SBOM despite failed diff.
if ! diff -sty --left-column -W 200 old.json new.json >|diff.txt; then
  declare status
  status='{"status":"failed", "type":"test", "should_continue":true, "desc":"detected significant changes in Augmented SBOM"}'
  curl -sS -d "${status:?}" -H "Content-Type: application/json" -X POST localhost:2285/task_status || true
fi

cat diff.txt

echo "Comparing Augmented SBOM... done."
