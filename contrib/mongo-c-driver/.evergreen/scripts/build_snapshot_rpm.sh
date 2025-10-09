#!/usr/bin/env bash

set -o errexit

#
# build_snapshot_rpm.sh
#

#
# Copyright 2018 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


for arg in "$@"; do
  if [ "$arg" = "-h" ]; then
    echo "Usage: ./.evergreen/scripts/build_snapshot_rpm.sh"
    echo ""
    echo "  This script is used to build a .rpm package directly from a snapshot of the"
    echo "  current repository."
    echo ""
    echo "  This script must be called from the base directory of the repository, and"
    echo "  requires utilities from these packages: rpm-build, mock"
    echo ""
    exit
  fi
done

package=mongo-c-driver
spec_file=../mongo-c-driver.spec
config=${MOCK_TARGET_CONFIG:=fedora-40-aarch64}

if [ ! -x /usr/bin/rpmbuild -o ! -x /usr/bin/rpmspec ]; then
  echo "Missing the rpmbuild or rpmspec utility from the rpm-build package"
  exit 1
fi

if [ ! -d .git ]; then
  echo "This script only works from within a repository"
  exit 1
fi

if [ ! -x /usr/bin/mock ]; then
  echo "Missing mock"
  exit 1
fi

if [ -f "${spec_file}" ]; then
  echo "Found old spec file (${spec_file})...removing"
  rm -f  ${spec_file}
fi
cp "$(pwd)/.evergreen/etc/${package}.spec" ..
if [ -f .evergreen/etc/spec.patch ]; then
  patch -d .. -p0 -i $(pwd)/.evergreen/etc/spec.patch
fi

changelog_package=$(rpmspec --srpm -q --qf "%{name}" ${spec_file})
if [ "${package}" != "${changelog_package}" ]; then
  echo "This script is configured to create snapshots for ${package} but you are trying to create a snapshot for ${changelog_package}"
  exit 1
fi

build_dir=$(basename $(pwd))

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --init
mock_root=$(sudo mock -r ${config} --use-bootstrap-image --isolation=simple --print-root-path)
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --install rpmdevtools git rpm-build cmake python3.11 gcc openssl-devel

# This step is needed to avoid the following error on rocky+epel8:
# Problem: conflicting requests
#  - package utf8proc-devel-2.6.1-3.module+el8.7.0+1065+42200b2e.aarch64 from powertools is filtered out by modular filtering
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --dnf-cmd --setopt=powertools.module_hotfixes=true install utf8proc-devel

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyin "$(pwd)" "$(pwd)/${spec_file}" /tmp
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- /bin/sh -c "(
  python3.11 build/calc_release_version.py | sed -E 's/([^-]+).*/\1/' > VERSION_CURRENT ;
  python3.11 build/calc_release_version.py -p > VERSION_RELEASED
  )"
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyout "/tmp/${build_dir}/VERSION_CURRENT" "/tmp/${build_dir}/VERSION_RELEASED" .

bare_upstream_version=$(rpmspec --srpm -q --qf '%{version}' "$spec_file")
# Upstream version in the .spec file cannot have hyphen (-); replace the current
# version so that the dist tarball version does not have a pre-release component
sudo sh -c "echo ${bare_upstream_version} > VERSION_CURRENT"
echo "Found bare upstream version: ${bare_upstream_version}"
git_rev="$(git rev-parse --short HEAD)"
snapshot_version="${bare_upstream_version}-0.$(date +%Y%m%d)+git${git_rev}"
echo "Upstream snapshot version: ${snapshot_version}"
current_package_version=$(rpmspec --srpm -q --qf "%{version}-%{release}" ${spec_file})

if [ -n "${current_package_version##*${git_rev}*}" ]; then
  echo "Making RPM changelog entry"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- rpmdev-bumpspec --comment="Built from Git Snapshot." --userstring="Test User <test@example.com>" --new="${snapshot_version}%{?dist}" ${spec_file}
fi

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyout "/tmp/${build_dir}/${spec_file}" ..

[ -d ~/rpmbuild/SOURCES ] || mkdir -p ~/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

# Create a source archive for rpmbuild to use:
expect_filename=$(rpmspec --srpm -q --qf '%{source}' "$spec_file")
tar_filestem=$package-$bare_upstream_version
tar_filename=$tar_filestem.tar
tar_filepath="/tmp/$tar_filename"
tgz_filepath="$HOME/rpmbuild/SOURCES/$expect_filename"
echo "Creating source archive [$tgz_filepath]"
# If Evergreen is running a patch build, changes have been git applied to the index.
# Commit the changes to include them in the tarball.
git commit -m "Include applied changes from a patch build" || true
git archive --format=tar --output="$tar_filepath" --prefix="$tar_filestem/" HEAD
mkdir -p "$tar_filestem"
cp VERSION_CURRENT "$tar_filestem/."
tar -rf "$tar_filepath" "$tar_filestem/"
gzip --keep "$tar_filepath" --stdout > "$tgz_filepath"

echo "Building source RPM ..."
rpmbuild -bs ${spec_file}
echo "Building binary RPMs ..."
mock_result=$(readlink -f ../mock-result)
sudo mock --resultdir="${mock_result}" --use-bootstrap-image --isolation=simple -r ${config} --no-clean --no-cleanup-after --rebuild ~/rpmbuild/SRPMS/${package}-${snapshot_version}*.src.rpm || ( cd "${mock_result}" ; cat *.log ; exit 1 )
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyin "${mock_result}" /tmp

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- /bin/sh -c "(
  rpm -Uvh ../mock-result/*.rpm &&
  gcc -I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -o example-client src/libmongoc/examples/example-client.c -lmongoc-1.0 -lbson-1.0
  )"

if [ ! -e "${mock_root}/tmp/${build_dir}/example-client" ]; then
  echo "Example was not built!"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
  exit 1
fi

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
(cd "${mock_result}" ; tar zcvf ../rpm.tar.gz *.rpm)
