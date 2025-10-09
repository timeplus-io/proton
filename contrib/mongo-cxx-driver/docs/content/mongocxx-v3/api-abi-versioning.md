+++
date = "2016-08-15T16:11:58+05:30"
title = "API and ABI versioning"
[menu.main]
  weight = 20
  parent="mongocxx3"
+++

## API Versioning

* We use [semantic versioning](http://semver.org/).
* bsoncxx and mongocxx both define corresponding CMake variables for MAJOR, MINOR, and PATCH.

## ABI Versioning

* Both bsoncxx and mongocxx both have a single scalar ABI version.
* Only bump ABI version on **incompatible** ABI change (not for ABI additions).
* **We stay on ABI version \_noabi (without bumping for incompatible changes) until ABI is stable.**

## Parallel Header Installation

* For mongocxx, install all headers to `$PREFIX/mongocxx/v$ABI/`.
* For bsoncxx, install all headers to `$PREFIX/bsoncxx/v$ABI/`.
* We install a pkg-config file to shield consumers from this complexity.

## Sonames and symlinks

*Note that below examples are given for libmongocxx, but also apply to libbsoncxx*

*DSO = Dynamic Shared Object, to use Ulrich Drepper's terminology*

### Windows (MSVC only)

Since version 3.10.0, the physical filename for CXX Driver libraries is different
from other platforms when built with the MSVC toolchain on Windows
(even when the CMake generator is not Visual Studio).
To restore prior behavior, which is similar to other platforms, set `ENABLE_ABI_TAG_IN_LIBRARY_FILENAMES=OFF`.

* Physical filename for a DSO is `mongocxx-$ABI-$TAG.dll`
* Physical filename for a static library is `mongocxx-static-$TAG.lib`

Where `$TAG` is a triplet of letters indicating:

* Build Type
* mongoc Link Type
* Polyfill Library

followed by a suffix describing the toolset and runtime library used to build the library.

Some examples of common DSO filenames expected to be generated include:

* `mongocxx-v_noabi-rhs-x64-v142-md.dll` (release build configuration)
* `mongocxx-v_noabi-dhs-x64-v142-mdd.dll` (debug build configuration)
* `mongocxx-v_noabi-rts-x64-v142-md.dll` (link with mongoc statically)
* `mongocxx-v_noabi-rhi-x64-v142-md.dll` (bsoncxx polyfill library)
* `mongocxx-v_noabi-rhm-x64-v142-md.dll` (mnmlstc/core polyfill library)
* `mongocxx-v_noabi-rhb-x64-v142-md.dll` (Boost polyfill library)

This allows libraries built with different build configurations (and different runtime library requirements) to be built and installed without conflicting with each other.

See references to `ENABLE_ABI_TAG_IN_LIBRARY_FILENAMES` and related code in the CMake configuration for more details.

### Other Platforms (Linux, MacOS)

* Physical filename for a DSO is `libmongocxx.so.$MAJOR.$MINOR.$PATCH`

Note that the physical filename is disconnected from ABI version/soname.
This looks a bit strange, but allows multiple versions of the library with
the same ABI version to be installed on the same system.

* soname for a DSO is `libmongocxx.$ABI`

We provide a soname symlink that links to the physical DSO.  We also
provide a dev symlink that links to the soname symlink of the highest ABI
version of the library installed.

## Inline namespaces

* We provide inline namespace macros for both mongocxx and bsoncxx.
* This allows multiple, ABI incompatible versions of the library to be linked into the same application.
* The name of the namespace is `v$ABI`. We create them from ABI version to maintain forwards compatibibility.


## Deprecation

Occasionally we will phase features out of use in the driver.
In the release that marks a feature as deprecated we offer several transition options:

1. The original method, marked with `MONGOCXX_DEPRECATED`. This
will raise deprecation warnings when compiled.
2. A variant of the feature suffixed with `_deprecated`. This will require only small
code changes and will not raise deprecation warnings.
3. A new feature that provides alternate functionality.
The new feature may not be a drop-in replacement for the deprecated feature and
switching to it may require code changes. In the rare case where we
remove a feature without replacing it, this third option will not be available.

In the following release, the original feature marked with `MONGOCXX_DEPRECATED` and its
suffixed `_deprecated` equivalent will be removed.

```c++
// release 1 with a supported feature
void do_thing();

// release 2 where the feature is deprecated in favor of a new feature
MONGOCXX_DEPRECATED void do_thing();
void do_thing_deprecated();
void do_new_thing();

// release 3 where the original feature is removed
void do_new_thing();
```

In the case of a feature rename we do not offer a suffixed `_deprecated` variant,
as one can simply switch to using the new name with the same amount of effort.

```
// release 1 with a supported feature
void do_thing();

// release 2 where the feature name is renamed
MONGOCXX_DEPRECATED void do_thing();
void do_stuff();

// release 3 where the original feature name is removed
void do_stuff();
```
