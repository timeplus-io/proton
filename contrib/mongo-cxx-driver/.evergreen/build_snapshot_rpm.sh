#!/bin/sh

set -o errexit

#
# build_snapshot_rpm.sh
#

#
# Copyright 2020 MongoDB, Inc.
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
    echo "Usage: ./.evergreen/build_snapshot_rpm.sh"
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

package=mongo-cxx-driver
spec_file=../mongo-cxx-driver.spec
config=${MOCK_TARGET_CONFIG:=fedora-39-aarch64}

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
cp "$(pwd)/.evergreen/${package}.spec" ..
if [ -f .evergreen/spec.patch ]; then
  patch -d .. -p0 -i $(pwd)/.evergreen/spec.patch
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
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --install rpmdevtools git rpm-build cmake utf8proc boost-devel openssl-devel cyrus-sasl-devel libbson-devel mongo-c-driver-devel snappy-devel gcc-c++ libzstd-devel
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyin "$(pwd)" "$(pwd)/${spec_file}" /tmp
if [ ! -f build/VERSION_CURRENT ]; then
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- /bin/sh -c "(
    python3 etc/calc_release_version.py | sed -E 's/([^-]+).*/\1/' > build/VERSION_CURRENT
    )"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyout "/tmp/${build_dir}/build/VERSION_CURRENT" build/
fi

bare_upstream_version=$(sed -E 's/([^-]+).*/\1/' build/VERSION_CURRENT)
# Upstream version in the .spec file cannot have hyphen (-); replace the current
# version so that the dist tarball version does not have a pre-release component
sudo sh -c "echo ${bare_upstream_version} > build/VERSION_CURRENT"
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

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- /bin/sh -c "(
  [ -d build ] || mkdir build ;
  cd build ;
  /usr/bin/cmake -DCMAKE_BUILD_TYPE=Release -DBSONCXX_POLY_USE_BOOST=1 -DENABLE_UNINSTALL=OFF .. ;
  make -j 8 dist
  )"

[ -d build ] || mkdir build
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyout "/tmp/${build_dir}/build/${package}*.tar.gz" build

[ -d ~/rpmbuild/SOURCES ] || mkdir -p ~/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
mv build/${package}*.tar.gz ~/rpmbuild/SOURCES/

echo "Building source RPM ..."
rpmbuild -bs ${spec_file}
echo "Building binary RPMs ..."
mock_result=$(readlink -f ../mock-result)
sudo mock --resultdir="${mock_result}" --use-bootstrap-image --isolation=simple -r ${config} --no-clean --no-cleanup-after --rebuild ~/rpmbuild/SRPMS/${package}-${snapshot_version}*.src.rpm || ( cd "${mock_result}" ; cat *.log ; exit 1 )
sudo mock -r ${config} --use-bootstrap-image --isolation=simple --copyin "${mock_result}" /tmp

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --cwd "/tmp/${build_dir}" --chroot -- /bin/sh -c "(
  rpm -Uvh ../mock-result/*.rpm &&
  /usr/bin/g++ -I/usr/include/bsoncxx/v_noabi -I/usr/include/mongocxx/v_noabi -o runcommand_examples examples/mongocxx/mongodb.com/runcommand_examples.cpp -lmongocxx -lbsoncxx &&
  /usr/bin/g++ -I/usr/include/bsoncxx/v_noabi -I/usr/include/mongocxx/v_noabi -o aggregation_examples examples/mongocxx/mongodb.com/aggregation_examples.cpp -lmongocxx -lbsoncxx &&
  /usr/bin/g++ -I/usr/include/bsoncxx/v_noabi -I/usr/include/mongocxx/v_noabi -o index_examples examples/mongocxx/mongodb.com/index_examples.cpp -lmongocxx -lbsoncxx &&
  /usr/bin/g++ -I/usr/include/bsoncxx/v_noabi -I/usr/include/mongocxx/v_noabi -o documentation_examples examples/mongocxx/mongodb.com/documentation_examples.cpp -lmongocxx -lbsoncxx
  )"

if [ ! -e "${mock_root}/tmp/${build_dir}/runcommand_examples" ]; then
  echo "Example was not built!"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
  exit 1
fi

if [ ! -e "${mock_root}/tmp/${build_dir}/aggregation_examples" ]; then
  echo "Example was not built!"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
  exit 1
fi

if [ ! -e "${mock_root}/tmp/${build_dir}/index_examples" ]; then
  echo "Example was not built!"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
  exit 1
fi

if [ ! -e "${mock_root}/tmp/${build_dir}/documentation_examples" ]; then
  echo "Example was not built!"
  sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
  exit 1
fi

sudo mock -r ${config} --use-bootstrap-image --isolation=simple --clean
(cd "${mock_result}" ; tar zcvf ../rpm.tar.gz *.rpm)

