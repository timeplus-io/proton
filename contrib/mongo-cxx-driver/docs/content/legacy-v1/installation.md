+++
date = "2016-08-15T16:11:58+05:30"
title = "Installing the legacy driver"
[menu.main]
  parent="legacy"
  weight = 5
+++

### Table of Contents

- [**How to ask for Help**](#how-to-ask-for-help)
- [Get the source code](#get-the-source-code)
- [Choose a branch](#choose-a-branch)
  - [legacy branch](#legacy-branch)
- [Compile the Driver](#compile-the-driver)
  - [SCons options when Compiling the C++ Driver](#scons-options-when-compiling-the-c-driver)
    - [Targets](#targets)
    - [Client Options](#client-feature-options)
    - [Path Options](#path-options)
    - [Build Options](#build-options)
    - [SCons Options](#scons-options)
    - [Windows Options](#windows-options)
    - [Mac OS X Options](#mac-os-x-options)
    - [Deprecated Options](#deprecated-options)
  - [Windows Considerations](#windows-considerations)
- [Example C++ Driver Compilations](#example-c-driver-compilations)
  - [Debug Builds](#debug-builds)
  - [Building on Windows](#building-on-windows)
    - [Building against the pre-built boost binaries](#building-against-the-pre-built-boost-binaries)
    - [Building a DLL](#building-a-dll-new-in-version-255)
    - [Building multiple Windows library variants](#building-multiple-windows-library-variants)
- [Using the driver in your application](#using-the-driver-in-your-application)
  - [Initialization and Configuration](#initialization-and-configuration)
  - [Client Headers](#client-headers)
  - [Client Libraries](#client-libraries)
    - [Windows autolinking](#windows-autolinking)
    - [Linking with the static client library](#linking-with-the-static-client-library)

### How to ask for Help

If you are having difficulty building the driver after reading the below
instructions, please post on the [MongoDB Community Forums](https://www.mongodb.com/community/forums/tag/cxx) to ask for
help. Please include in your post **all** of the following information:

 - The version of the driver you are trying to build (branch or tag).
   - Examples: _legacy-1.0.1 tag_, _legacy-1.0.2 tag_
 - Host OS, version, and architecture.
   - Examples: _Windows 8 64-bit_ x86, _Ubuntu 12.04 32-bit x86_, _OS X Mavericks_
 - C++ Compiler and version.
   - Examples: _GCC 4.8.2_, _MSVC 2013 Express_, _clang 3.4_, _XCode 5_
 - Boost version.
   - Examples: _boost 1.55_, _boost 1.49_
 - How boost was built or installed.
   - Examples: _apt-get install libboost-all-dev_, _built from source_, _windows binary install_
   - If you built boost from source, please include your boost build invocation as well.
 - The complete SCons invocation.
   - Example: _scons -j10 install_
 - The output of the configure phase of the build. If the configure phase failed (e.g. boost was not found), please attach the contents of the file build/scons/config.log.
 - The error you encountered. This may be compiler, SCons, or other output.

Failure to include the relevant information will result in additional
round-trip communications to ascertain the necessary details, delaying a
useful response. Here is a made-up example of a help request that provides
the relevant information:

**PLEASE NOTE: The build invocation below is incomplete and intentionally
erroneous. Read the section on building against the pre-built boost
binaries under the "Building on Windows" section to understand what is
wrong here, and the rest of the page to learn about other important options
you will want or need to use when building the driver.**

---

*I'm trying to build the legacy-0.9 tag on Windows 8 64-bit, using MSVC
2013. I have the boost 1.55 pre-built Windows binaries for VC12 installed
to D:\local\boost-1.55. When I invoked scons as `scons --mute --64
--extrapath=D:\local\boost-1.55`, the configure step will not find the
boost headers. The build gives the following configure output*:

```
Checking whether the C++ compiler works yes
Checking whether the C compiler works yes
Checking if C++ compiler "$CC" is MSVC... yes
Checking if C compiler "cl" is MSVC... yes
Checking if we are using libstdc++... no
WARNING: Cannot disable C++11 features when using MSVC
Checking if we are on a POSIX system... no
Checking for __declspec(thread)... yes
Checking for C++ header file boost/version.hpp... no
Could not find boost headers in include search path
```

_Why can't the build system find the boost headers?_

_Thanks_

---

While collecting this information will take some additional time and
effort, providing it will make it much more likely for your question to
receive a prompt and immediately helpful reply.

### Prerequisites
 - [Boost](http://www.boost.org/) (>= 1.49) # May work with older versions back to 1.41
   - NOTE: On systems offering multiple C++ standard libraries, you must ensure that the standard library
     linked into boost matches that linked into the driver.
 - [Python](https://www.python.org/) (2.x)
 - [SCons](http://www.scons.org/)
 - [Git](http://git-scm.com/)

### Get the Source Code

To get a repository that you can build, you can clone the sources, and then
check out the branch or tag that you are interested in:

```sh
git clone -b releases/legacy https://github.com/mongodb/mongo-cxx-driver.git
```

Alternatively, see the
[releases](https://github.com/mongodb/mongo-cxx-driver/releases) page for
downloadable tarball files containing the files associated with each
released tag.

### Choose a Branch

#### Legacy Branch:

Use the [legacy](https://github.com/mongodb/mongo-cxx-driver/tree/legacy) branch if:

- You are using MongoDB's C++ driver for the first time.
- You had been using 26compat (or the driver inside of the server source) and want to benefit from incremental improvements while having the same overall API.

```
git checkout legacy
```

### Compile the Driver

From the directory where you cloned the code, compile the C++ driver by
running the `scons` command. Use the SCons options described in this
section.

To see the list of all SCons options, run: `scons --help`

#### SCons Options when Compiling the C++ Driver

Select options as appropriate for your environment. Please note that some
flags may not be available on older versions.

**Important note about C++11/C++14**: The boost libraries do not offer a
stable ABI across different versions of the C++ standard. As a result, you
must ensure that your application, the C++ driver, and boost are all built
with the same language standard. In particular, if you are building the C++
driver with C++11 enabled, you must also build your application with C++11
enabled, and link against a C++11 compiled boost. Note that on most
systems, the system or package installed boost distribution is *not* built
with C++11, and is therefore incompatible with a C++11 build of the legacy
driver.

**Important note about the C++ standard library**: Much like the C++11
issues, it is again critical that all three components (your application,
boost, and the C++ driver) be built against the same C++ runtime library.
You cannot mix components that have linked against libc++ with those that
have linked against libstdc++.

##### Targets

There are several targets you can build, but the most common target for users of the library is `install`, which will build the driver, and install the driver and headers to the location specified with the `--prefix` argument. If no prefix is specified, `--prefix` defaults to a directory named ```build/install``` under the current source directory.

##### Client Feature Options
 - `--ssl` Enables SSL support. You will need a compatible version of the SSL libraries available.The default authorization mechanism since MongoDB version 3.0 is [SCRAM-SHA-1](https://www.mongodb.com/docs/manual/core/security-scram/). If you want to use standard MongoDB authentication, you should compile with --ssl option for SCRAM-SHA-1 mechanism support.
 - `--use-sasl-client` Enables SASL, which MongoDB uses for the Kerberos authentication available on MongoDB Enterprise. You will need a compatible version of the SASL implementation libraries available. The Cyrus SASL libraries are what we test with, and are recommended.
 - `--sharedclient` Builds a shared library version of the client driver alongside the static library. If applicable for your application, prefer using the shared client.

##### Path Options
 - `--prefix=<path>` The directory prefix for the installation directory. Set <path> to the directory where you want the build artifacts (headers and library files) installed. For example, you might set <path> to `/opt/local`, `/usr/local`, or `$HOME/mongo-client-install`.
 - `--libpath=<path-to-libs>` Specifies path to additional libraries.
 - `--cpppath=<path-to-headers>` Specifies path to additional headers.
 - `--extrapath=<path-to-boost>` Specifies the path to your Boost libraries if they are not in a standard search path for your toolchain.
 - `--runtime-library-search-path` Specifies the runtime search path for dynamic libraries when running tests. Set this to the directory containing boost, ssl, or sasl DLLs as required.
   - NOTE: This option is only available on the `legacy` branch at version legacy-0.10.0-pre or later. Prior to legacy-rc1, this option is available under the older `--dllpath` name.

##### Build Options
 - `--cc` The compiler to use for C. Use the following syntax: `--cc=<path-to-c-compiler>`
 - `--cxx` The compiler to use for C++. Use the following syntax: `--cxx=<path-to-c++-compiler>`
 - `--dbg=[on|off]` Enables runtime debugging checks. Defaults to off. Specifying `--dbg=on` implies `--opt=off` unless explicitly overridden with `--opt=on`.
 - `--opt=[on|off]` Enables compile-time optimization. Defaults to on. Can be freely mixed with the values for the `--dbg` flag.
 - `--c++11=[on|off]` Builds the driver in C++11 mode. Defaults to off. Please see the note above about requirements for using C++11.
 - `--libc++` Builds the driver against the libc++ C++ runtime library. Please see the note above about requirements for the C++ runtime library.

##### Scons Options
 - `--cache` Enables caching of object files.
 - `-j N` Compile with N cores.

##### Windows Options
 - `--dynamic-windows` By default, on Windows, compilation uses `/MT`. Use this flag to compile with `/MD`. Note that `/MD` is required to build the shared client on Windows. Also note that your application compiler flags must match. If you build with `--dbg=on`, `/MTd` or `/MDd` will be used in place of `/MT` or `/MD`, respectively.
 - `--dynamic-boost=[on|off|auto]` Selects whether to link the driver to the boost libraries dynamically, statically, or as dynamic iff `--dynamic-windows` is enabled, respectively.
 - `--win-version-min` Override the default build system choice of minimum windows version to target. Allowable options are currently `xpsp3`, `ws03sp2`, `vista`, `ws08r2`, `win7`, and `win8`.
 - `--msvc-host-arch` Override the detected host architecture. The allowable choices are `x86`, `i386`, `amd64`, `emt64`, `x86_64`, and `ia64`. You should only need to use this if the auto-detected host architecture selects a compiler variant that is not available on your system.
 - `--msvc-script=[<path>]` Explicitly selects an MSVC configuration script to run. This may allow you to use MSVC toolchain versions for which your version of SCons does not offer support. You may also pass the empty string to inhibit SCons execution of any MSVC configuration scrip to run. This is useful if you have a "dressed" MSVC shell that you prefer to use. In this case, you will also need to pass the `--propagate-shell-environment` flag to the build so that the shell environment variables are passed down to the tool invocation.
 - `--msvc-version` Explicitly select the MSVC version. This is useful if you have multiple toolchains installed. By default SCons will select the newest. If you need to run an older toolchain, you may override with this flag. Please be aware that the value passed here is the VC version (like 10.0, 11.0, 12.0, etc.), not the VS version (2010, 2012, etc.).

##### Mac OS X Options (Mac OS X Only)
 - `--osx-version-min=[10.7|10.8|10.9]` Minimum version of Mac OS X to build for. Use `--osx-version-min=10.9` when compiling on OS X 10.9 Mavericks to automatically select `libc++` as the default runtime library, which is necessary if the prerequisite libraries (e.g. Boost) are built against `libc++`.

#### Windows Considerations

When building on Windows, use of the SCons `--dynamic-windows` option can
result in an error unless all libraries and sources for the application use
the same C runtime library. This option builds the driver to link against
the dynamic link C RTL instead of the static C RTL. If the Boost library
being linked against is expecting an `/MT` build (static C RTL), this can
result in an error similar to the following:

```
error LNK2005: ___ already defined in msvcprt.lib(MSVCP100.dll) libboost_thread-vc100-mt-1_42.lib(thread.obj)
```

The same caveat applies to building with the --dbg=on flag, which will
select the debug runtime library.

You may want to define _CRT_SECURE_NO_WARNINGS to avoid warnings on use of
strncpy and such by the MongoDB client code.

Include the WinSock library in your application: Linker ‣ Input ‣
Additional Dependencies. Add ws2_32.lib.

### Example C++ Driver Compilations

The following are examples of building the C++ driver.

The following example installs the driver to `$HOME/mongo-client-install`:

```sh
scons --prefix=$HOME/mongo-client-install install
```

To enable SSL, add the `--ssl` option:

```sh
scons --prefix=$HOME/mongo-client-install --ssl install
```

To enable SASL support for use with Kerberos authentication on MongoDB Enterprise, add the `--use-sasl-client` option:

```sh
scons --prefix=$HOME/mongo-client-install --use-sasl-client install
```

To build a shared library version of the driver, along with the normal static library, use the `--sharedclient` option:

```sh
scons --prefix=$HOME/mongo-client-install --sharedclient install
```

To use a custom version of boost installed to /dev/boost, use the `--extrapath=<path-to-boost>` option:

```sh
scons --prefix=$HOME/mongo-client-install --extrapath=/dev/boost install
```

To target OS X 10.9 Mavericks (and default to using `libc++`), use the `--osx-version-min=<version>` option:

```sh
scons --prefix=$HOME/mongo-client-install --osx-version-min=10.9 install
```

##### Debug Builds

To build a version of the library with debugging enabled, use `--dbg=on`.
This turns off optimization, which is on by default. To enable both
debugging and optimization, pass `--dbg=on --opt=on`:

```sh
scons --prefix=$HOME/mongo-client-install --dbg=on --opt=on install
```

To override the default compiler to a newer GCC installed in `/opt/local/gcc-4.8`, use the `--cc` and `--cxx` options:

```sh
scons --prefix=$HOME/mongo-client-install --cc=<path-to-gcc> --cxx=<path-to-g++> install
```

##### Building on Windows

###### Building against the pre-built boost binaries.

Building boost from source can be challenging on Windows. If appropriate for your situation, we recommend using the [pre built boost Windows binaries](http://sourceforge.net/projects/boost/files/boost-binaries/). Please note that you must select a download that properly reflects your target architecture (i.e. 32-bit or 64-bit) and toolchain revision (MSVC 10, 11, etc. Note that this is the VC version **not** the Visual Studio version).

Due to the layout of the boost installation in the pre-built binaries, you cannot use the `--extrapath` SCons flag to inform the build of the installation path for the boost binaries. Instead, you should use the `--cpppath` flag to point to the root of the chosen boost installation path, and `--libpath` to point into the appropriately named library subdirectory of the boost installation. For example, if you have installed the 64-bit boost 1.55 libraries for MSVC11 into `D:\local\boost_1_55_0_msvc11`, then you would add

```
--cpppath=d:\local\boost_1_55_0_msvc --libpath=d:\local\boost_1_55_0_msvc11\lib64-msvc-11.0
````

to your SCons invocation.

###### Building a DLL (New in version 2.5.5)

```sh
scons
    <--64 or --32>
    --sharedclient
    --dynamic-windows
    --prefix=<install-path>
    --cpppath=<path-to-boost-headers>
    --libpath=<path-to-boost-libs>
    install
```

###### The following example will build and install the C++ driver, in a PowerShell:

```sh
scons
    --64
    --sharedclient
    --dynamic-windows
    --prefix="%HOME%\mongo-client-install"
    --cpppath="C:\local\boost_1_55_0\include"
    --libpath="C:\local\boost_1_55_0\lib64-msvc-12.0"
    install
```

###### Building multiple Windows library variants:

As of legacy-0.8, the Windows libraries are now tagged with boost-like ABI tags (see http://www.boost.org/doc/libs/1_55_0/more/getting_started/windows.html#library-naming), so it is possible to build several different variants (debug vs retail, static vs dynamic runtime) and install them to the same location. We have added support for autolib, so the selection of the appropriate library is handled automatically (see https://jira.mongodb.com/browse/CXX-200). To build all of the different driver variants, repeatedly invoke scons as follows:

```
scons $ARGS install
scons $ARGS install --dbg=on
scons $ARGS install --dynamic-windows --sharedclient
scons $ARGS install --dynamic-windows --sharedclient --dbg=on
```

Where ```$ARGS``` are the arguments you would normally pass (e.g. ```--cpppath```, ```--libpath```, ```--64```, ```--prefix```, etc.). You should ensure that you use the same arguments for all four invocations. If this works properly, your ```$PREFIX/lib``` directory should contain the following files:

```
libmongoclient.lib
libmongoclient-gd.lib
libmongoclient-s.lib
libmongoclient-sgd.lib
mongoclient.dll
mongoclient.exp
mongoclient.lib
mongoclient.pdb
mongoclient-gd.dll
mongoclient-gd.exp
mongoclient-gd.lib
mongoclient-gd.pdb
```

### Using the driver in your application

#### Initialization and Configuration

NOTE: You *must* initialize the legacy driver before use.  See
[Configuration]({{< ref "/legacy-v1/configuration" >}}) for more details.

#### Client Headers

There are only two headers intended for direct inclusion by consumers of the library:

- `$PREFIX/include/mongo/bson/bson.h`
- `$PREFIX/include/mongo/client/dbclient.h`

These 'facade' headers should include all of the headers necessary to use
the driver or BSON library. Directly including other headers from
`$PREFIX/include/mongo/` is unlikely to work as intended and may lead to
subtle or hard to diagnose problems.

To consume the headers, you must configure your build system so that
`$PREFIX/include` is incorporated into the header search path. For
GCC-esque compilers, this is typically done with the `-I` flag. For IDEs,
consult the relevant documentation on how to configure the header search
path for your project.

#### Client Libraries

Depending on how you built the driver, you may end up with a static
library, a dynamic library, or both. On Windows, you may end up with many
libraries with different names.

To link with the library, you must configure your build system so that
`$PREFIX/lib` is incorporated into the link-time library search path. For
GCC-esque compilers, this is typically done with the `-L` flag. For IDEs,
consult the relevant documentation on how to configure the library search
path for your project.

Once you have added the search path, you may need to specify the name of
the library you want to link against on your application link line. For GCC
style compilers, this is typically done with the `-l` flag. For IDE's,
consult the relevant documentation on how to add libraries to the link
line.

##### Windows autolinking

For `legacy-0.9.0+` on Windows, the driver off autolib support. In this
case, you do not need to add the client library as a dependent library.
Inclusion of the client headers will register the dependency on the library
and it will automatically be linked. You do still need to specify the
library search path however.

##### Linking with the static client library.

If you intend to link against the static client library, you must also
define the preprocessor symbol `STATIC_LIBMONGOCLIENT` in all translation
units that include the driver or BSON headers. Failure to do so will result
in hard to diagnose warnings or errors.
