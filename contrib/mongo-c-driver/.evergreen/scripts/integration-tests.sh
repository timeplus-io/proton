#! /bin/bash
# Start up mongo-orchestration (a server to spawn mongodb clusters) and set up a cluster.
#
# Specify the following environment variables:
#
# MONGODB_VERSION: latest, 4.2, 4.0, 3.6
# TOPOLOGY: server, replica_set, sharded_cluster
# AUTH: auth, noauth
# AUTHSOURCE
# IPV4_ONLY: off, on
# SSL: openssl, darwinssl, winssl, nossl
# ORCHESTRATION_FILE: <file name in orchestration_configs/ without .json>
#   If this is not set, the file name is constructed from other options.
# OCSP: off, on
# REQUIRE_API_VERSION: set to a non-empty string to set the requireApiVersion parameter
#   This is currently only supported for standalone servers
# LOAD_BALANCER: off, on
#
# This script may be run locally.
#

set -o errexit  # Exit the script with error if any of the commands fail

: "${MONGODB_VERSION:="latest"}"
: "${LOAD_BALANCER:="off"}"

DIR=$(dirname $0)
# mongoc/.evergreen/scripts -> drivers-evergreen-tools/.evergreen/download-mongodb.sh
. $DIR/../../../drivers-evergreen-tools/.evergreen/download-mongodb.sh

get_distro
get_mongodb_download_url_for "$DISTRO" "$MONGODB_VERSION"
DRIVERS_TOOLS=./ download_and_extract "$MONGODB_DOWNLOAD_URL" "$EXTRACT" "$MONGOSH_DOWNLOAD_URL" "$EXTRACT_MONGOSH"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
TOPOLOGY=${TOPOLOGY:-server}
OCSP=${OCSP:-off}
REQUIRE_API_VERSION=${REQUIRE_API_VERSION}

# If caller of script specifies an ORCHESTRATION_FILE, do not attempt to modify it. Otherwise construct it.
if [ -z "$ORCHESTRATION_FILE" ]; then
   ORCHESTRATION_FILE="basic"

   if [ "$AUTH" = "auth" ]; then
      ORCHESTRATION_FILE="auth"
   fi

   if [ "$IPV4_ONLY" = "on" ]; then
      ORCHESTRATION_FILE="${ORCHESTRATION_FILE}-ipv4-only"
   fi

   if [ -n "$AUTHSOURCE" ]; then
      ORCHESTRATION_FILE="${ORCHESTRATION_FILE}-${AUTHSOURCE}"
   fi

   if [ "$SSL" != "nossl" ]; then
      ORCHESTRATION_FILE="${ORCHESTRATION_FILE}-ssl"
   fi

   if [ "$LOAD_BALANCER" = "on" ]; then
      ORCHESTRATION_FILE="${ORCHESTRATION_FILE}-load-balancer"
   fi
fi

# Set up mongo orchestration home.
case "$OS" in
   cygwin*)
      export MONGO_ORCHESTRATION_HOME="c:/data/MO"
      FULL_PATH=$(cygpath -m -a .)
      ;;
   *)
      export MONGO_ORCHESTRATION_HOME=$(pwd)"/MO"
      FULL_PATH=$(pwd)
      ;;
esac
rm -rf "$MONGO_ORCHESTRATION_HOME"
mkdir -p "$MONGO_ORCHESTRATION_HOME/lib"
mkdir -p "$MONGO_ORCHESTRATION_HOME/db"

# Replace ABSOLUTE_PATH_REPLACEMENT_TOKEN with path to mongo-c-driver.
find orchestration_configs -name \*.json | xargs perl -p -i -e "s|ABSOLUTE_PATH_REPLACEMENT_TOKEN|$FULL_PATH|g"

# mongo-orchestration expects client.pem to be in MONGO_ORCHESTRATION_HOME. So always copy it.
cp -f src/libmongoc/tests/x509gen/* $MONGO_ORCHESTRATION_HOME/lib/
# find print0 and xargs -0 not available on Solaris. Lets hope for good paths
find orchestration_configs -name \*.json | xargs perl -p -i -e "s|/tmp/orchestration-home|$MONGO_ORCHESTRATION_HOME/lib|g"

export ORCHESTRATION_FILE="orchestration_configs/${TOPOLOGY}s/${ORCHESTRATION_FILE}.json"
export ORCHESTRATION_URL="http://localhost:8889/v1/${TOPOLOGY}s"

export TMPDIR=$MONGO_ORCHESTRATION_HOME/db
echo From shell `date` > $MONGO_ORCHESTRATION_HOME/server.log

command -V "${PYTHON3_BINARY:?}" >/dev/null

# shellcheck source=/dev/null
. ../drivers-evergreen-tools/.evergreen/venv-utils.sh

case "$OS" in
   cygwin*)
      # Python has problems with unix style paths in cygwin. Must use c:\\ paths
      rm -rf /cygdrive/c/mongodb
      cp -r mongodb /cygdrive/c/mongodb
      echo "{ \"releases\": { \"default\": \"c:\\\\mongodb\\\\bin\" }}" > orchestration.config

      # Make sure MO is running latest version
      venvcreate "${PYTHON3_BINARY}" venv
      cd venv
      rm -rf mongo-orchestration
      git clone --depth 1 https://github.com/10gen/mongo-orchestration.git
      cd mongo-orchestration
      python -m pip install .
      cd ../..
      nohup mongo-orchestration -f orchestration.config -e default --socket-timeout-ms=60000 --bind=127.0.0.1  --enable-majority-read-concern -s wsgiref start > $MONGO_ORCHESTRATION_HOME/out.log 2> $MONGO_ORCHESTRATION_HOME/err.log < /dev/null &
      ;;
   *)
      echo "{ \"releases\": { \"default\": \"`pwd`/mongodb/bin\" } }" > orchestration.config

      venvcreate "${PYTHON3_BINARY}" venv
      cd venv
      rm -rf mongo-orchestration
      # Make sure MO is running latest version
      git clone --depth 1 https://github.com/10gen/mongo-orchestration.git
      cd mongo-orchestration
      # Our zSeries machines are static-provisioned, cache corruptions persist.
      if [ $(uname -m) = "s390x" ]; then
         echo "Disabling pip cache"
         PIP_PARAM="--no-cache-dir"
      fi
      python -m pip $PIP_PARAM install .
      cd ../..
      mongo-orchestration -f orchestration.config -e default --socket-timeout-ms=60000 --bind=127.0.0.1  --enable-majority-read-concern start > $MONGO_ORCHESTRATION_HOME/out.log 2> $MONGO_ORCHESTRATION_HOME/err.log < /dev/null &
      ;;
esac

echo "Waiting for mongo-orchestration to start..."
wait_for_mongo_orchestration() {
   for i in $(seq 300); do
      # Exit code 7: "Failed to connect to host".
      if curl -s "localhost:$1" 1>|curl_mo.txt; test $? -ne 7; then
         return 0
      else
         sleep 1
      fi
   done
   echo "Could not detect mongo-orchestration on port $1"
   return 1
}
wait_for_mongo_orchestration 8889
echo "Waiting for mongo-orchestration to start... done."

python -m json.tool curl_mo.txt
sleep 5
pwd
curl -s --data @"$ORCHESTRATION_FILE" "$ORCHESTRATION_URL" 1>|curl_mo.txt
python -m json.tool curl_mo.txt
sleep 15

if [ "$AUTH" = "auth" ]; then
  MONGO_SHELL_CONNECTION_FLAGS="--username bob --password pwd123"
fi

if [ -n "$AUTHSOURCE" ]; then
   MONGO_SHELL_CONNECTION_FLAGS="${MONGO_SHELL_CONNECTION_FLAGS} --authenticationDatabase ${AUTHSOURCE}"
fi

if [ "$OCSP" != "off" ]; then
   MONGO_SHELL_CONNECTION_FLAGS="${MONGO_SHELL_CONNECTION_FLAGS} --host localhost --tls --tlsAllowInvalidCertificates"
elif [ "$SSL" != "nossl" ]; then
   MONGO_SHELL_CONNECTION_FLAGS="${MONGO_SHELL_CONNECTION_FLAGS} --host localhost --ssl --sslCAFile=$MONGO_ORCHESTRATION_HOME/lib/ca.pem --sslPEMKeyFile=$MONGO_ORCHESTRATION_HOME/lib/client.pem"
fi

if [ ! -z "$REQUIRE_API_VERSION" ]; then
  MONGO_SHELL_CONNECTION_FLAGS="${MONGO_SHELL_CONNECTION_FLAGS} --apiVersion=1"
  # Set the requireApiVersion parameter.
  ./mongodb/bin/mongosh $MONGO_SHELL_CONNECTION_FLAGS $DIR/../etc/require-api-version.js
fi

echo $MONGO_SHELL_CONNECTION_FLAGS

# Create mo-expansion.yml. expansions.update expects the file to exist.
touch mo-expansion.yml

if [ -z "$MONGO_CRYPT_SHARED_DOWNLOAD_URL" ]; then
  echo "There is no crypt_shared library for distro='$DISTRO' and version='$MONGODB_VERSION'".
else
  echo "Downloading crypt_shared package from $MONGO_CRYPT_SHARED_DOWNLOAD_URL"
  download_and_extract_crypt_shared "$MONGO_CRYPT_SHARED_DOWNLOAD_URL" "$EXTRACT" "CRYPT_SHARED_LIB_PATH"
  echo "CRYPT_SHARED_LIB_PATH: $CRYPT_SHARED_LIB_PATH"
  if [ -z "$CRYPT_SHARED_LIB_PATH" ]; then
    echo "CRYPT_SHARED_LIB_PATH must be assigned, but wasn't" 1>&2 # write to stderr"
    exit 1
  fi
cat >>mo-expansion.yml <<EOT
CRYPT_SHARED_LIB_PATH: "$CRYPT_SHARED_LIB_PATH"
EOT

fi
