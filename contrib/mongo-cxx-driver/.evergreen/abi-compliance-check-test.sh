#!/usr/bin/env bash

set -o errexit
set -o pipefail

declare working_dir
working_dir="$(pwd)"

declare base current
base="$(cat base-commit.txt)"
current="$(cat current-commit.txt)"

export PATH
PATH="${working_dir:?}/install/bin:${PATH:-}"

# Remove 'r' prefix in version string.
declare old_ver new_ver
old_ver="${base:1}-base"
new_ver="${current:1}-current"

command -V abi-compliance-checker >/dev/null

mkdir cxx-abi cxx-noabi

# Generate XML descriptors with sections used by both old and new tests.
if true; then
  cat >old.xml <<DOC
<version>
  ${old_ver:?}
</version>

<libs>
  ../install/old/lib
</libs>

<add_include_paths>
  ../install/old/include/
</add_include_paths>

DOC

  cat >new.xml <<DOC
<version>
  ${new_ver:?}
</version>

<libs>
  ../install/new/lib
</libs>

<add_include_paths>
  ../install/new/include/
</add_include_paths>

DOC

  {
    cat <<DOC
<skip_including>
  bsoncxx/v_noabi/bsoncxx/enums/
  bsoncxx/v_noabi/bsoncxx/config/
</skip_including>

<skip_namespaces>
  bsoncxx::detail
  bsoncxx::v_noabi::detail
  bsoncxx::v_noabi::stdx::detail
</skip_namespaces>

DOC
  } | tee -a old.xml new.xml >/dev/null

  cat old.xml | tee cxx-abi/old.xml cxx-noabi/old.xml >/dev/null
  cat new.xml | tee cxx-abi/new.xml cxx-noabi/new.xml >/dev/null
  rm old.xml new.xml
fi

# Append sections specific to each ABI test.
if true; then
  cat >>cxx-abi/old.xml <<DOC
<headers>
  ../install/old/include/bsoncxx/
  ../install/old/include/mongocxx/
</headers>

<skip_headers>
  /v_noabi/
</skip_headers>
DOC

  cat >>cxx-noabi/old.xml <<DOC
<headers>
  ../install/old/include/bsoncxx/v_noabi
  ../install/old/include/mongocxx/v_noabi
</headers>
DOC

  cat >>cxx-abi/new.xml <<DOC
<headers>
  ../install/new/include/mongocxx/
  ../install/new/include/bsoncxx/
</headers>

<skip_headers>
  /v_noabi/
</skip_headers>
DOC

  cat >>cxx-noabi/new.xml <<DOC
<headers>
  ../install/new/include/bsoncxx/v_noabi
  ../install/new/include/mongocxx/v_noabi
</headers>
DOC
fi

# Allow task to upload the HTML report despite failed status.
echo "Generating stable ABI report..."
pushd cxx-abi
if ! abi-compliance-checker -lib mongo-cxx-driver -old old.xml -new new.xml; then
  : # CXX-2812: enable code below once stable ABI symbols exist in the base commit libraries.
  # declare status
  # status='{"status":"failed", "type":"test", "should_continue":true, "desc":"abi-compliance-checker emitted one or more errors"}'
  # curl -sS -d "${status:?}" -H "Content-Type: application/json" -X POST localhost:2285/task_status || true
fi
popd # cxx-abi
echo "Generating stable ABI report... done."

# Also create a report for the unstable ABI. Errors are expected and OK.
echo "Generating unstable ABI report..."
pushd cxx-noabi
abi-compliance-checker -lib mongo-cxx-driver -old old.xml -new new.xml || true
popd # cxx-noabi
echo "Generating unstable ABI report... done."
