+++
date = "2020-09-26T13:14:52-04:00"
title = "Advanced Configuration and Installation Options"
[menu.main]
  identifier = "mongocxx3-installation-advanced"
  parent = "mongocxx3-installation"
  weight = 34
+++

## Additional Options for Integrators

In the event that you are building the BSON C++ library and/or the C++ driver to embed with other components and you wish to avoid the potential for collision with components installed from a standard build or from a distribution package manager, you can make use of the `BSONCXX_OUTPUT_BASENAME` and `MONGOCXX_OUTPUT_BASENAME` options to `cmake`.

```sh
cmake ..                                            \
    -DBSONCXX_OUTPUT_BASENAME=custom_bsoncxx        \
    -DMONGOCXX_OUTPUT_BASENAME=custom_mongocxx
```

The above command would produce libraries named `libcustom_bsoncxx.so` and `libcustom_mongocxx.so` (or with the extension appropriate for the build platform).  Those libraries could be placed in a standard system directory or in an alternate location and could be linked to by specifying something like `-lcustom_mongocxx -lcustom_bsoncxx` on the linker command line (possibly adjusting the specific flags to those required by your linker).

## Installing the MongoDB C driver.

The mongocxx driver builds on top of the [MongoDB C driver](https://www.mongodb.com/docs/drivers/c/).

The build of mongocxx-3.9.0 automatically downloads and installs the C driver if the C driver is not detected.
To use an existing install of the C driver, set `CMAKE_PREFIX_PATH` to the directory containing the C driver install.

* For mongocxx-3.10.x, libmongoc 1.25.0 or later is required.
* For mongocxx-3.9.x, libmongoc 1.25.0 or later is required.
* For mongocxx-3.8.x, libmongoc 1.24.0 or later is required.
* For mongocxx-3.7.x, libmongoc 1.22.1 or later is required.
* For mongocxx-3.6.x, libmongoc 1.17.0 or later is required.
* For mongocxx-3.5.x, libmongoc 1.15.0 or later is required.
* For mongocxx-3.4.x, libmongoc 1.13.0 or later is required.
* For mongocxx-3.3.x, libmongoc 1.10.1 or later is required.
* For mongocxx-3.2.x, libmongoc 1.9.2 or later is required.
* For mongocxx-3.1.4+, libmongoc 1.7.0 or later is required.
* For mongocxx-3.1.[0-3], libmongoc 1.5.0 or later is required.
* For mongocxx-3.0.x, we recommend the last 1.4.x version of libmongoc

Unless you know that your package manager offers a sufficiently recent version, you
will need to download and build from the source code. Get a tarball from
the [C Driver releases](https://github.com/mongodb/mongo-c-driver/releases)
page.

Follow the instructions for building from a tarball at
[Installing libmongoc](http://mongoc.org/libmongoc/current/installing.html).

Industry best practices and some regulations require the use of TLS 1.1
or newer. The MongoDB C Driver supports TLS 1.1 on Linux if OpenSSL is
at least version 1.0.1. On macOS and Windows, the C Driver uses native
TLS implementations that support TLS 1.1.

## Advanced configuration (static configurations)

The following sub-sections detail advanced options for configuring the C++ driver and/or its
dependencies as static libraries rather than the typical shared libraries.  These options will
produce library artifacts that will behave differently.  Ensure you have a complete understanding
of the implications of the various linking approaches before utilizing these options.

### Configuring with `mongocxx` 3.2.x or newer

Users have the option to build `mongocxx` as a static library. **This is not recommended for novice
users.** A user can enable this behavior with the `-DBUILD_SHARED_LIBS` option:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DBUILD_SHARED_LIBS=OFF
```

### Configuring with `mongocxx` 3.5.0 or newer

Users have the option to build `mongocxx` as both static and shared libraries. A user can enable
this behavior with the `-DBUILD_SHARED_AND_STATIC_LIBS` option:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DBUILD_SHARED_AND_STATIC_LIBS=ON
```

Users have the option to build `mongocxx` as a shared library that has statically linked
`libmongoc`. **This is not recommended for novice users.** A user can enable this behavior with the
`-DBUILD_SHARED_LIBS_WITH_STATIC_MONGOC` option:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DBUILD_SHARED_LIBS_WITH_STATIC_MONGOC=ON
```

## Disabling tests

Pass `-DENABLE_TESTS=OFF` as a cmake option to disable configuration of test targets.

```sh
cmake .. -DENABLE_TESTS=OFF
cmake --build .. --target help
# No test targets are configured.
```

## Installing to non-standard directories

To install the C++ driver to a non-standard directory, specify `CMAKE_INSTALL_PREFIX` to the desired
install path:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DCMAKE_INSTALL_PREFIX=$HOME/mongo-cxx-driver
```

Consider also specifying the `-DCMAKE_INSTALL_RPATH=` option to the `lib` directory of the install.
This may enable libmongocxx.so to locate libbsoncxx.so:

```sh
cmake ..                                             \
    -DCMAKE_BUILD_TYPE=Release                       \
    -DCMAKE_INSTALL_PREFIX=$HOME/mongo-cxx-driver    \
    -DCMAKE_INSTALL_RPATH=$HOME/mongo-cxx-driver/lib
```

If the C driver is installed to a non-standard directory, specify `CMAKE_PREFIX_PATH` to the install
path of the C driver:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DCMAKE_PREFIX_PATH=$HOME/mongo-c-driver        \
    -DCMAKE_INSTALL_PREFIX=$HOME/mongo-cxx-driver
```

> *Note* If you need multiple paths in a CMake PATH variable, separate them with a semicolon like
> this:
> `-DCMAKE_PREFIX_PATH="/your/cdriver/prefix;/some/other/path"`

### Configuring with `mongocxx` 3.1.x or 3.0.x

Instead of the `-DCMAKE_PREFIX_PATH` option, users must specify the `libmongoc` installation
directory by using the `-DLIBMONGOC_DIR` and `-DLIBBSON_DIR` options:

```sh
cmake ..                                            \
    -DCMAKE_BUILD_TYPE=Release                      \
    -DLIBMONGOC_DIR=$HOME/mongo-c-driver            \
    -DLIBBSON_DIR=$HOME/mongo-c-driver              \
    -DCMAKE_INSTALL_PREFIX=$HOME/mongo-cxx-driver
```

### Fixing the "Library not loaded" error on macOS

Applications linking to a non-standard directory installation may encounter an error loading the C++ driver at runtime. Example:

```sh
# Tell pkg-config where to find C++ driver installation.
export PKG_CONFIG_PATH=$HOME/mongo-cxx-driver/lib/pkgconfig
clang++ app.cpp -std=c++11 $(pkg-config --cflags --libs libmongocxx) -o ./app.out
./app.out
# Prints the following error:
# dyld[3217]: Library not loaded: '@rpath/libmongocxx._noabi.dylib'
#   Referenced from: '/Users/kevin.albertson/code/app.out'
#   Reason: tried: '/usr/local/lib/libmongocxx._noabi.dylib' (no such file), '/usr/lib/libmongocxx._noabi.dylib' (no such file)
# zsh: abort      ./app.out
```

The default `install name` of the C++ driver on macOS includes `@rpath`:
```sh
otool -D $HOME/mongo-cxx-driver/lib/libmongocxx.dylib
# Prints:
# /Users/kevin.albertson/mongo-cxx-driver/lib/libmongocxx.dylib:
# @rpath/libmongocxx._noabi.dylib
```

Including `@rpath` in the install name allows linking applications to control the list of search paths for the library.

`app.out` includes the load command for `@rpath/libmongocxx._noabi.dylib`. `app.out` does not have entries to substitute for `@rpath`.

There are several ways to consider solving this on macOS:

Pass `DYLD_FALLBACK_LIBRARY_PATH` to the directory containing the C++ driver libraries:

```sh
DYLD_FALLBACK_LIBRARY_PATH=$HOME/mongo-cxx-driver/lib ./app.out
# Prints "successfully connected with C++ driver"
```

Alternatively, the linker option `-Wl,-rpath` can be passed to add entries to substitute for `@rpath`:
```sh
# Tell pkg-config where to find C++ driver installation.
export PKG_CONFIG_PATH=$HOME/mongo-cxx-driver/lib/pkgconfig
# Pass the linker option -rpath to set an rpath in the final executable.
clang++ app.cpp -std=c++11 -Wl,-rpath,$HOME/mongo-cxx-driver/lib $(pkg-config --cflags --libs libmongocxx) -o ./app.out
./app.out
# Prints "successfully connected with C++ driver"
```

If building the application with cmake, the [Default RPATH settings](https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#default-rpath-settings) include the full RPATH to all used libraries in the build tree. However, when installing, cmake will clear the RPATH of these targets so they are installed with an empty RPATH. This may result in a `Library not loaded` error after install.

Example:
```sh
# Build application `app` using the C++ driver from a non-standard install.
cmake \
    -DCMAKE_PREFIX_PATH=$HOME/mongo-cxx-driver \
    -DCMAKE_INSTALL_PREFIX=$HOME/app \
    -DCMAKE_CXX_STANDARD=11 \
    -Bcmake-build -S.
cmake --build cmake-build --target app.out
# Running app.out from build tree includes rpath to C++ driver.
./cmake-build ./cmake-build/app.out
# Prints: "successfully connected with C++ driver"

cmake --build cmake-build --target install
# Running app.out from install tree does not include rpath to C++ driver.
$HOME/app/bin/app.out
# Prints "Library not loaded" error.
```

Consider setting `-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE` so the rpath for the executable is kept in the install target.
```sh
# Build application `app` using the C++ driver from a non-standard install.
# Use CMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE to keep rpath entry on installed app.
cmake \
    -DCMAKE_PREFIX_PATH=$HOME/mongo-cxx-driver \
    -DCMAKE_INSTALL_PREFIX=$HOME/app \
    -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE \
    -DCMAKE_CXX_STANDARD=11 \
    -Bcmake-build -S.

cmake --build cmake-build --target install
$HOME/app/bin/app.out
# Prints "successfully connected with C++ driver"
```

See the cmake documentation for [RPATH handling](https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling) for more information.

### Fixing the "cannot open shared object file" error on Linux

Applications linking to a non-standard directory installation may encounter an error loading the C++ driver at runtime. Example:

```sh
# Tell pkg-config where to find C++ driver installation.
export PKG_CONFIG_PATH=$HOME/mongo-cxx-driver/lib/pkgconfig
g++ -std=c++11 app.cpp $(pkg-config --cflags --libs libmongocxx) -o ./app.out
./app.out
# Prints the following error:
# ./app.out: error while loading shared libraries: libmongocxx.so._noabi: cannot open shared object file: No such file or directory
```

There are several ways to consider solving this on Linux:

Pass `LD_LIBRARY_PATH` to the directory containing the C++ driver libraries:

```sh
LD_LIBRARY_PATH=$HOME/mongo-cxx-driver/lib ./app.out
# Prints "successfully connected with C++ driver"
```

Alternatively, the linker option `-Wl,-rpath` can be passed to add `rpath` entries:
```sh
# Tell pkg-config where to find C++ driver installation.
export PKG_CONFIG_PATH=$HOME/mongo-cxx-driver/lib/pkgconfig
# Pass the linker option -rpath to set an rpath in the final executable.
g++ app.cpp -std=c++11 -Wl,-rpath,$HOME/mongo-cxx-driver/lib $(pkg-config --cflags --libs libmongocxx) -o ./app.out
./app.out
# Prints "successfully connected with C++ driver"
```

If building the application with cmake, the [Default RPATH settings](https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#default-rpath-settings) include the full RPATH to all used libraries in the build tree. However, when installing, cmake will clear the RPATH of these targets so they are installed with an empty RPATH. This may result in a `Library not loaded` error after install.

Example:
```sh
# Build application `app` using the C++ driver from a non-standard install.
cmake \
    -DCMAKE_PREFIX_PATH=$HOME/mongo-cxx-driver \
    -DCMAKE_INSTALL_PREFIX=$HOME/app \
    -DCMAKE_CXX_STANDARD=11 \
    -Bcmake-build -S.
cmake --build cmake-build --target app.out
# Running app.out from build tree includes rpath to C++ driver.
./cmake-build ./cmake-build/app.out
# Prints: "successfully connected with C++ driver"

cmake --build cmake-build --target install
# Running app.out from install tree does not include rpath to C++ driver.
$HOME/app/bin/app.out
# Prints "cannot open shared object file" error.
```

Consider setting `-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE` so the rpath for the executable is kept in the install target.
```sh
# Build application `app` using the C++ driver from a non-standard install.
# Use CMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE to keep rpath entry on installed app.
cmake \
    -DCMAKE_PREFIX_PATH=$HOME/mongo-cxx-driver \
    -DCMAKE_INSTALL_PREFIX=$HOME/app \
    -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=TRUE \
    -DCMAKE_CXX_STANDARD=11 \
    -Bcmake-build -S.

cmake --build cmake-build --target install
$HOME/app/bin/app.out
# Prints "successfully connected with C++ driver"
```

See the cmake documentation for [RPATH handling](https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling) for more information.
