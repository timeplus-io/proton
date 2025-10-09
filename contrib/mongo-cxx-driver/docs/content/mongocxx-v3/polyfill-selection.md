+++
date = "2024-02-20T00:00:00-00:00"
title = "Choosing a C++17 Polyfill"
[menu.main]
  identifier = "mongocxx3-polyfill-selection"
  parent = "mongocxx3"
  weight = 8
+++

The mongocxx driver uses C++17 features `std::optional` and `std::string_view`.
To use the C++17 standard library implementations for these features, set
the CMake configuration variable `CMAKE_CXX_STANDARD` to 17 or higher.
Otherwise, to compile the mongocxx driver for pre-C++17 configurations, a
polyfill library implementation must be selected from the following options
(note: "default" refers to **pre-C++17** configurations when no polyfill library
is explicitly selected):

* bsoncxx (*default only when `-DENABLE_BSONCXX_POLY_USE_IMPLS=ON`*)

  Select with `-DBSONCXX_POLY_USE_IMPLS=ON`. This option is most recommended, as
  it does not require additional external library dependencies. To enable
  selecting this option by default for pre-C++17 configurations when no other
  options are specified, set `ENABLE_BSONCXX_POLY_USE_IMPLS=ON` (this option
  will be set to ON by default in an upcoming major release).

* MNMLSTC/core (*default for non-Windows platforms*)

  **This option is deprecated and will be removed in an upcoming major release.**
  Select with `-DBSONCXX_POLY_USE_MNMLSTC=1`. **NOTE**: This option vendors a
  header-only installation of MNMLSTC/core into the bsoncxx library installation
  and will therefore download MLNMLSTC from GitHub during the configuration
  process. If you already have an available version of MNMLSTC on your system,
  you can avoid the download step by using `-DBSONCXX_POLY_USE_SYSTEM_MNMLSTC`.

* Boost (*default for Windows platforms*)

  **This option is deprecated and will be removed in an upcoming major release.**
  Select with `-DBSONCXX_POLY_USE_BOOST=1`. This is currently the only
  non-bsoncxx option if you are using a version of MSVC that does not support
  C++17.

Most users should use default behavior with `-DENABLE_BSONCXX_POLY_USE_IMPLS=ON`.
However, if you have a preference for one of the external polyfill libraries
(e.g. already a dependency being used by your application), you may prefer to
explicitly select that external polyfill library rather than rely on default
selection behavior.

**NOTE**: C++ standard conformance and supported behavior of polyfill features
may vary depending on the selected polyfill library. The purpose of these
polyfills is to support pre-C++17 configurations by providing stand-ins for
their C++17 equivalents. Therefore we recommend using the C++17 standard
library whenever possible by setting `-DCMAKE_CXX_STANDARD=17` or newer.

**WARNING**: the choice of polyfill library has a direct impact on the public
API and ABI for the mongocxx library. Changing the polyfill can lead to both
source-breaking changes (during compilation) and binary-breaking changes (during
linking or execution). To limit reliance on polyfill-specific configuration or
behavior, avoid using `stdx::string_view` and `stdx::optional<T>` with
non-mongocxx library interfaces.
