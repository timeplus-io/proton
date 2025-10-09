+++
date = "2020-09-26T13:47:18-04:00"
title = "macOS"
[menu.main]
  identifier = "mongocxx3-installation-macos"
  parent = "mongocxx3-installation"
  weight = 32
+++

### Step 1: Choose a C++17 polyfill

First, [choose a C++17 polyfill library]({{< ref "/mongocxx-v3/polyfill-selection" >}}).

### Step 2: Download the latest version of the mongocxx driver.

The most reliable starting point for building the mongocxx driver is the latest
release tarball.

The [mongocxx releases](https://github.com/mongodb/mongo-cxx-driver/releases)
page will have links to the release tarball for the version you wish you install.  For
example, to download version 3.10.0:

```sh
curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.10.0/mongo-cxx-driver-r3.10.0.tar.gz
tar -xzf mongo-cxx-driver-r3.10.0.tar.gz
cd mongo-cxx-driver-r3.10.0/build
```

Make sure you change to the `build` directory of whatever source tree you
obtain.

### Step 3: Configure the driver

Without additional
configuration, `mongocxx` installs into its local build directory as a courtesy to those who build
from source. To configure `mongocxx` for installation into `/usr/local` as well, use the following
`cmake` command:

```
cmake ..                                \
    -DCMAKE_BUILD_TYPE=Release          \
    -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF
```

These options can be freely mixed with a C++17 polyfill option. For instance, this is how a user
would run the command above with the Boost polyfill option:
```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DBSONCXX_POLY_USE_BOOST=1                      \
    -DMONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX=OFF
```

### Step 4: Build and install the driver

Build and install the driver:

```sh
cmake --build .
sudo cmake --build . --target install
```

The driver can be uninstalled at a later time in one of two ways.  First,
the uninstall target can be called:

```sh
sudo cmake --build . --target uninstall
```

Second, the uninstall script can be called:

```sh
sudo <install-dir>/share/mongo-cxx-driver/uninstall.sh
```
