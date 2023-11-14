#!/bin/bash

# GitHub user/repo
USER_REPO="timeplus-io/proton"

# Fetch the latest release tag from GitHub
LATEST_TAG=$(curl -s https://api.github.com/repos/$USER_REPO/releases/latest | grep 'tag_name' | cut -d\" -f4)

# Check if the tag is empty
if [ -z "$LATEST_TAG" ]; then
  echo "Failed to fetch the latest release tag from GitHub." >&2
  exit 1
fi

# Identify the system's OS and architecture
OS=$(uname -s)
ARCH=$(uname -m)

# Map the architecture to the binary naming convention
case $ARCH in
  "x86_64")
    ARCH="x86_64"
    ;;
  "arm64" | "aarch64")
    if [ "$OS" == "Darwin" ]; then
      ARCH="arm64"
    else
      ARCH="aarch64"
    fi
    ;;
  *)
    echo "Currently, github.com/timeplus-io/proton does not support $OS-$ARCH releases. You can try our docker image\
            with  \
            \$ docker pull ghcr.io/timeplus-io/proton" >&2
    exit 1
    ;;
esac

# Binary file name
BINARY_FILE="proton-${LATEST_TAG}-${OS}-${ARCH}"

# Download URL
DOWNLOAD_URL="https://github.com/$USER_REPO/releases/download/${LATEST_TAG}/${BINARY_FILE}"

# Download the binary
echo "Downloading $BINARY_FILE..."
curl -L -o "$BINARY_FILE" "$DOWNLOAD_URL"

# Check if the download was successful
if [ $? -eq 0 ]; then
  echo "Download completed: $BINARY_FILE"
  echo "
To interact with Proton:
0. Give Proton executable permissions:
   sudo chmod u+x $BINARY_FILE

1. Start the Proton server:
   ./$BINARY_FILE server start

2. In a separate terminal, connect to the server:
   ./$BINARY_FILE client
   (Note: If you encounter a 'connection refused' error, use: ./$BINARY_FILE client --host 127.0.0.1)

3. To terminate the server, press ctrl+c in the server terminal.

For detailed usage and more information, check out the Timeplus documentation:
https://docs.timeplus.com/"
else
  echo "Download failed or the binary for $OS-$ARCH is not available." >&2
fi
