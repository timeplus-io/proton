#!/bin/bash

SLING_VERSION=1.2.20
SLING_VERSION_SUFFIX=timeplus.3

arch=$(uname -m)
if [ "$arch" == "x86_64" ]; then
    SLING_ARCH=amd64
else
    SLING_ARCH=arm64
fi

SLING_ZIP=sling_${SLING_VERSION}-${SLING_VERSION_SUFFIX}_linux_${SLING_ARCH}.zip
SLING_URL=https://github.com/timeplus-io/sling-cli/releases/download/v${SLING_VERSION}-${SLING_VERSION_SUFFIX}/${SLING_ZIP}

run_and_echo() {
  echo "+ $*"
  "$@"
}

run_and_echo wget $SLING_URL

run_and_echo docker build . -f Dockerfile.sling --build-arg SLING_ZIP=$SLING_ZIP -t timeplus/sling-cli:$SLING_VERSION-$SLING_VERSION_SUFFIX
