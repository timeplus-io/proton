# 3rd Party Dependency Vulnerabilities

This document tracks vulnerabilities in 3rd party dependencies that directly affect the MongoDB C++ Driver standard release product.

> [!IMPORTANT]
> The "standard release product" is defined as the set of files which are _installed_ by a configuration, build, and install of the MongoDB C++ Driver. This includes static/shared library files, header files, and packaging files for supported build configurations. Vulnerabilities for 3rd party dependencies that are bundled with the standard release product are reported in this document.
>
> The release distribution tarball (`.tar.gz`) and corresponding digital signature (`.tar.gz.asc`) which are uploaded to GitHub as release assets are also considered part of the standard release product.
>
> Test files, utility scripts, documentation generators, and other miscellaneous files and artifacts are NOT considered part of the standard release product, even if they are included in the release distribution tarball. Vulnerabilities for such 3rd party dependencies are NOT reported in this document.

## Template

This section provides a template that may be used for actual vulnerability reports further below.

### CVE-YYYY-NNNNNN

- **Date Detected:** YYYY-MM-DD
- **Severity:** Low, Medium, High, or Critical
- **Detector:** Silk or Snyk
- **Description:** A short vulnerability description.
- **Dependency:** Name and version of the 3rd party dependency.
- **Upstream Status:** False Positive, Won't Fix, Fix Pending, or Fix Available. This is the fix status for the 3rd party dependency, not the CXX Driver. "Fix Available" should include the version and/or date when the fix was released, e.g. "Fix Available (1.2.3, 1970-01-01)".
- **Fix Status:** False Positive, Won't Fix, Fix Pending, or Fix Committed. This is the fix status for the CXX Driver. "False Positive" and "Won't Fix" must include rationale in notes below.
- **For Release:** The CXX Driver release version for which the "Fix Status" above was last updated.
- **Notes:** Context or rationale for remediation, references to relevant issue trackers, etc.

## mnmlstc/core

> [!IMPORTANT]
> These vulnerabilities only affect the MongoDB C++ Driver product when it is configured to use the mnmlstc/core as the C++17 polyfill library. See [Choosing a C++17 Polyfill](https://www.mongodb.com/docs/languages/cpp/cpp-driver/current/polyfill-selection/).

None.

## mongodb/mongo-c-driver

None.
