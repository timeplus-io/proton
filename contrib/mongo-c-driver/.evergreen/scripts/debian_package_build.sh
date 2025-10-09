#!/bin/sh

#
# Test libmongoc's Debian packaging scripts.
#
# Supported/used environment variables:
#   IS_PATCH    If "true", this is an Evergreen patch build.

set -o errexit

on_exit () {
  if [ -e ./unstable-chroot/debootstrap/debootstrap.log ]; then
    echo "Dumping debootstrap.log (64-bit)"
    cat ./unstable-chroot/debootstrap/debootstrap.log
  fi
  if [ -e ./unstable-i386-chroot/debootstrap/debootstrap.log ]; then
    echo "Dumping debootstrap.log (32-bit)"
    cat ./unstable-i386-chroot/debootstrap/debootstrap.log
  fi
}
trap on_exit EXIT

git config user.email "evergreen-build@example.com"
git config user.name "Evergreen Build"

if [ "${IS_PATCH}" = "true" ]; then
  git diff HEAD > ../upstream.patch
  git clean -fdx
  git reset --hard HEAD
  git remote add upstream https://github.com/mongodb/mongo-c-driver
  git fetch upstream
  CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
  git checkout upstream/debian/unstable
  git checkout ${CURRENT_BRANCH}
  git checkout upstream/debian/unstable -- ./debian/
  if [ -s ../upstream.patch ]; then
    [ -d debian/patches ] || mkdir debian/patches
    mv ../upstream.patch debian/patches/
    echo upstream.patch >> debian/patches/series
    git add debian/patches/*
    git commit -m 'Evergreen patch build - upstream changes'
    git log -n1 -p
  fi
fi

cd ..

git clone https://salsa.debian.org/installer-team/debootstrap.git debootstrap.git
export DEBOOTSTRAP_DIR=`pwd`/debootstrap.git
sudo -E ./debootstrap.git/debootstrap --variant=buildd unstable ./unstable-chroot/ http://cdn-aws.deb.debian.org/debian
cp -a mongoc ./unstable-chroot/tmp/
sudo chroot ./unstable-chroot /bin/bash -c '(\
  apt-get install -y build-essential git-buildpackage fakeroot dpkg-dev debhelper cmake libssl-dev pkgconf python3-sphinx python3-sphinx-design furo libmongocrypt-dev zlib1g-dev libsasl2-dev libsnappy-dev libutf8proc-dev libzstd-dev libjs-mathjax && \
  chown -R root:root /tmp/mongoc && \
  cd /tmp/mongoc && \
  git clean -fdx && \
  git reset --hard HEAD && \
  git remote remove upstream || true && \
  git remote add upstream https://github.com/mongodb/mongo-c-driver && \
  git fetch upstream && \
  export CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)" && \
  git checkout upstream/debian/unstable && \
  git checkout ${CURRENT_BRANCH} && \
  git checkout upstream/debian/unstable -- ./debian/ && \
  git commit -m "fetch debian directory from the debian/unstable branch" && \
  LANG=C /bin/bash ./debian/build_snapshot.sh && \
  debc ../*.changes && \
  dpkg -i ../*.deb && \
  gcc -I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -o example-client src/libmongoc/examples/example-client.c -lmongoc-1.0 -lbson-1.0 )'

[ -e ./unstable-chroot/tmp/mongoc/example-client ] || (echo "Example was not built!" ; exit 1)
(cd ./unstable-chroot/tmp/ ; tar zcvf ../../deb.tar.gz *.dsc *.orig.tar.gz *.debian.tar.xz *.build *.deb)

# Build a second time, to ensure a "double build" works
sudo chroot ./unstable-chroot /bin/bash -c "(\
  cd /tmp/mongoc && \
  rm -f example-client && \
  git status --ignored && \
  dpkg-buildpackage -b && dpkg-buildpackage -S )"

# And now do it all again for 32-bit
sudo -E ./debootstrap.git/debootstrap --variant=buildd --arch i386 unstable ./unstable-i386-chroot/ http://cdn-aws.deb.debian.org/debian
cp -a mongoc ./unstable-i386-chroot/tmp/
sudo chroot ./unstable-i386-chroot /bin/bash -c '(\
  apt-get install -y build-essential git-buildpackage fakeroot dpkg-dev debhelper cmake libssl-dev pkgconf python3-sphinx python3-sphinx-design furo libmongocrypt-dev zlib1g-dev libsasl2-dev libsnappy-dev libutf8proc-dev libzstd-dev libjs-mathjax && \
  chown -R root:root /tmp/mongoc && \
  cd /tmp/mongoc && \
  git clean -fdx && \
  git reset --hard HEAD && \
  git remote remove upstream || true && \
  git remote add upstream https://github.com/mongodb/mongo-c-driver && \
  git fetch upstream && \
  export CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)" && \
  git checkout upstream/debian/unstable && \
  git checkout ${CURRENT_BRANCH} && \
  git checkout upstream/debian/unstable -- ./debian/ && \
  git commit -m "fetch debian directory from the debian/unstable branch" && \
  LANG=C /bin/bash ./debian/build_snapshot.sh && \
  debc ../*.changes && \
  dpkg -i ../*.deb && \
  gcc -I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -o example-client src/libmongoc/examples/example-client.c -lmongoc-1.0 -lbson-1.0 )'

[ -e ./unstable-i386-chroot/tmp/mongoc/example-client ] || (echo "Example was not built!" ; exit 1)
(cd ./unstable-i386-chroot/tmp/ ; tar zcvf ../../deb-i386.tar.gz *.dsc *.orig.tar.gz *.debian.tar.xz *.build *.deb)

# Build a second time, to ensure a "double build" works
sudo chroot ./unstable-i386-chroot /bin/bash -c "(\
  cd /tmp/mongoc && \
  rm -f example-client && \
  git status --ignored && \
  dpkg-buildpackage -b && dpkg-buildpackage -S )"
