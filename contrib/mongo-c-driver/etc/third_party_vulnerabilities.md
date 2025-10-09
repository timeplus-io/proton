# Thirty-Party Vulnerabilities

<!--

See docs/dev/deps.rst for an explanation of this file.

-->

This document lists known vulnerabilities in third-party dependencies that are
directly bundled with standard release product for the MongoDB C Driver.

This document was created on **$today** using data from
[Snyk Security](https://security.snyk.io), and the details herein reflect
information that was available at that time.

> [!IMPORTANT]
>
> The "standard release product" is defined as the set of files which are
> _installed_ by a configuration, build, and install. This includes
> static/shared library files, header files, and packaging files for supported
> build configurations.
>
> Vulnerabilities for 3rd party dependencies that are bundled with the standard
> release product are reported in this document. Test files, utility scripts,
> documentation generators, and other miscellaneous files and artifacts are NOT
> considered part of the standard release product, even if they are included in
> the release distribution tarball. Vulnerabilities for such 3rd party
> dependencies are NOT reported in this document.
>
> Details on packages that are not tracked tracked by Snyk Security will not
> appear in this document.

## `Zlib`

### CVE-2023-45853 - Integer Overflow or Wraparound

- **Date Detected**: 2024-06-24
- **CVE Number**: [CVE-2023-45853](https://www.cve.org/CVERecord?id=CVE-2023-45853)
- **Snyk Entry**: [SNYK-UNMANAGED-MADLERZLIB-5969359](https://security.snyk.io/vuln/SNYK-UNMANAGED-MADLERZLIB-5969359)
- **Severity**: High
- **Description**: Affected versions of this package are vulnerable to Integer
  Overflow or Wraparound via the `MiniZip` function in `zlib`, by providing a
  long filename, comment, or extra field.
- **Upstream Fix Status**: Fix available (1.3.1, 2024-01-22)
- **mongo-c-driver Fix Status**: Fix available (1.27.3, 2024-06-26)
- **Notes**: This issue was related to Zip file handling, which was not used by
  mongo-c-driver. This errant code was never reachable via the C driver APIs.

## `jsonsl`, `utf8proc`, and `uthash`

These bundled dependencies are present within the release archive, but are not
tracked by Snyk and therefore no vulnerability information is available.
