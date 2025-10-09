# Copyright 2018-present MongoDB, Inc.
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

from collections import OrderedDict as OD

from evergreen_config_generator.variants import Variant


mobile_flags = (
    " -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY"
    " -DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY"
    " -DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER"
    " -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY"
)


def days(n: int) -> int:
    "Calculate the number of minutes in the given number of days"
    return n * 24 * 60


all_variants = [
    Variant(
        "abi-compliance-check",
        "ABI Compliance Check",
        ["ubuntu1804-small", "ubuntu1804-medium", "ubuntu1804-large"],
        ["abi-compliance-check"],
    ),
    Variant(
        "smoke",
        "Smoke Tests",
        "ubuntu2204-small",
        [
            "make-docs",
            "kms-divergence-check",
            "release-compile",
            "debug-compile-no-counters",
            "compile-tracing",
            "link-with-cmake",
            "link-with-cmake-deprecated",
            "link-with-cmake-ssl",
            "link-with-cmake-ssl-deprecated",
            "link-with-cmake-snappy",
            "link-with-cmake-snappy-deprecated",
            OD([("name", "link-with-cmake-mac"), ("distros", ["macos-1100"])]),
            OD([("name", "link-with-cmake-mac-deprecated"), ("distros", ["macos-1100"])]),
            OD([("name", "link-with-cmake-windows"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "link-with-cmake-windows-ssl"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "link-with-cmake-windows-snappy"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "link-with-cmake-mingw"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "link-with-pkg-config"), ("distros", ["ubuntu1804-test"])]),
            OD([("name", "link-with-pkg-config-mac"), ("distros", ["macos-1100"])]),
            "link-with-pkg-config-ssl",
            "link-with-bson",
            OD([("name", "link-with-bson-windows"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "link-with-bson-mac"), ("distros", ["macos-1100"])]),
            OD([("name", "link-with-bson-mingw"), ("distros", ["windows-vsCurrent-large"])]),
            "check-headers",
            "install-uninstall-check",
            OD([("name", "install-uninstall-check-mingw"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "install-uninstall-check-msvc"), ("distros", ["windows-vsCurrent-large"])]),
            "debug-compile-with-warnings",
            OD([("name", "build-and-test-with-toolchain"), ("distros", ["debian10-small"])]),
            "install-libmongoc-after-libbson",
        ],
        {
            # Disable the MongoDB legacy shell download, which is not available in 5.0 for u22
            "SKIP_LEGACY_SHELL": "1"
        },
        tags=["pr-merge-gate"],
    ),
    Variant(
        "clang38",
        "clang 3.8 (Debian 9.2)",
        "debian92-test",
        ["release-compile", "debug-compile-nosasl-nossl", ".latest .nossl"],
        {"CC": "clang"},
    ),
    Variant(
        "openssl",
        "OpenSSL / LibreSSL",
        "archlinux-build",
        [
            "build-and-run-authentication-tests-openssl-1.0.1",
            "build-and-run-authentication-tests-openssl-1.0.2",
            "build-and-run-authentication-tests-openssl-1.1.0",
            "build-and-run-authentication-tests-openssl-1.0.1-fips",
            "build-and-run-authentication-tests-libressl-2.5",
            "build-and-run-authentication-tests-libressl-3.0-auto",
            "build-and-run-authentication-tests-libressl-3.0",
        ],
        {},
    ),
    Variant(
        "clang37",
        "clang 3.7 (Archlinux)",
        "archlinux-test",
        [
            "release-compile",
            "debug-compile-sasl-openssl",
            "debug-compile-nosasl-openssl",
            ".authentication-tests .openssl",
        ],
        {"CC": "clang"},
    ),
    Variant(
        "clang60-i686",
        "clang 6.0 (i686) (Ubuntu 18.04)",
        "ubuntu1804-test",
        [
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-no-align",
            ".debug-compile !.sspi .nossl .nosasl",
            ".latest .nossl .nosasl",
        ],
        {"CC": "clang", "MARCH": "i686"},
    ),
    Variant(
        "clang38-i686",
        "clang 3.8 (i686) (Ubuntu 16.04)",
        "ubuntu1604-test",
        ["release-compile", "debug-compile-no-align"],
        {"CC": "clang", "MARCH": "i686"},
    ),
    Variant(
        "clang38ubuntu",
        "clang 3.8 (Ubuntu 16.04)",
        "ubuntu1604-test",
        [
            ".compression !.zstd",
            "release-compile",
            "debug-compile-sasl-openssl",
            "debug-compile-nosasl-openssl",
            "debug-compile-no-align",
            ".authentication-tests .openssl",
        ],
        {"CC": "clang"},
    ),
    Variant(
        "gcc82rhel",
        "GCC 8.2 (RHEL 8.0)",
        "rhel80-test",
        [
            ".hardened",
            ".compression !.snappy !.zstd",
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-nosasl-openssl",
            "debug-compile-sasl-openssl",
            ".authentication-tests .openssl",
            ".latest .nossl",
        ],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc48rhel",
        "GCC 4.8 (RHEL 7.0)",
        "rhel70",
        # Skip client-side-encryption tests on RHEL 7.0 due to OCSP errors
        # with Azure. See CDRIVER-3620 and CDRIVER-3814.
        [
            ".hardened",
            ".compression !.snappy",
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-sasl-openssl",
            "debug-compile-nosasl-openssl",
            ".authentication-tests .openssl",
            ".latest .nossl",
        ],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc63",
        "GCC 6.3 (Debian 9.2)",
        "debian92-test",
        ["release-compile", "debug-compile-nosasl-nossl", ".latest .nossl"],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc83",
        "GCC 8.3 (Debian 10.0)",
        "debian10-test",
        ["release-compile", "debug-compile-nosasl-nossl", ".latest .nossl"],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc102",
        "GCC 10.2 (Debian 11.0)",
        "debian11-large",
        ["release-compile", "debug-compile-nosasl-nossl", ".latest .nossl"],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc94",
        "GCC 9.4 (Ubuntu 20.04)",
        "ubuntu2004-large",
        ["release-compile", "debug-compile-nosasl-nossl", ".latest .nossl"],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc75-i686",
        "GCC 7.5 (i686) (Ubuntu 18.04)",
        "ubuntu1804-test",
        ["release-compile", "debug-compile-nosasl-nossl", "debug-compile-no-align", ".latest .nossl .nosasl"],
        {"CC": "gcc", "MARCH": "i686"},
    ),
    Variant(
        "gcc75",
        "GCC 7.5 (Ubuntu 18.04)",
        "ubuntu1804-test",
        [
            ".compression !.zstd",
            "debug-compile-nosrv",
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-no-align",
            "debug-compile-sasl-openssl",
            "debug-compile-nosasl-openssl",
            ".authentication-tests .openssl",
            ".authentication-tests .asan",
            ".test-coverage",
            ".latest .nossl",
            "retry-true-latest-server",
            "test-dns-openssl",
            "test-dns-auth-openssl",
            "test-dns-loadbalanced-openssl",
        ],
        {"CC": "gcc"},
    ),
    Variant(
        "gcc54",
        "GCC 5.4 (Ubuntu 16.04)",
        "ubuntu1604-test",
        [".compression !.zstd", "debug-compile-nosrv", "release-compile", "debug-compile-no-align"],
        {"CC": "gcc"},
    ),
    Variant(
        "darwin",
        "*Darwin, macOS (Apple LLVM)",
        "macos-1100",
        [
            ".compression !.snappy",
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-rdtscp",
            "debug-compile-no-align",
            "debug-compile-nosrv",
            "debug-compile-sasl-darwinssl",
            "debug-compile-nosasl-nossl",
            ".authentication-tests .darwinssl",
            ".latest .nossl",
            "test-dns-darwinssl",
            "test-dns-auth-darwinssl",
            "debug-compile-lto",
            "debug-compile-lto-thin",
            "debug-compile-aws",
            "test-aws-openssl-regular-4.4",
            "test-aws-openssl-regular-latest",
        ],
        {"CC": "clang"},
    ),
    Variant(
        "windows-2017-32",
        "Windows (i686) (VS 2017)",
        "windows-vsCurrent-large",
        ["debug-compile-nosasl-nossl", ".latest .nossl .nosasl"],
        {"CC": "Visual Studio 15 2017"},
    ),
    Variant(
        "windows-2017",
        "Windows (VS 2017)",
        "windows-vsCurrent-large",
        [
            "debug-compile-nosasl-nossl",
            "debug-compile-nosasl-openssl",
            "debug-compile-sspi-winssl",
            ".latest .nossl",
            ".nosasl .latest .nossl",
            "test-dns-winssl",
            "test-dns-auth-winssl",
            "debug-compile-aws",
            "test-aws-openssl-regular-4.4",
            "test-aws-openssl-regular-latest",
            # Authentication tests with OpenSSL on Windows are only run on the vs2017 variant.
            # Older vs variants fail to verify certificates against Atlas tests.
            ".authentication-tests .openssl !.sasl",
            ".authentication-tests .winssl",
        ],
        {"CC": "Visual Studio 15 2017 Win64"},
    ),
    Variant(
        "windows-2015",
        "Windows (VS 2015)",
        "windows-64-vs2015-compile",
        [
            ".compression !.snappy !.zstd !.latest",
            "release-compile",
            "debug-compile-sspi-winssl",
            "debug-compile-no-align",
            "debug-compile-nosrv",
            ".authentication-tests .winssl",
        ],
        {"CC": "Visual Studio 14 2015 Win64"},
    ),
    Variant(
        "windows-2015-32",
        "Windows (i686) (VS 2015)",
        "windows-64-vs2015-compile",
        [
            ".compression !.snappy !.zstd !.latest",
            "release-compile",
            "debug-compile-sspi-winssl",
            "debug-compile-no-align",
            ".authentication-tests .winssl",
        ],
        {"CC": "Visual Studio 14 2015"},
    ),
    Variant(
        "windows-2013",
        "Windows (VS 2013)",
        "windows-64-vs2013-compile",
        [
            ".compression !.snappy !.zstd !.latest",
            "release-compile",
            "debug-compile-sspi-winssl",
            ".authentication-tests .winssl",
        ],
        {"CC": "Visual Studio 12 2013 Win64"},
    ),
    Variant(
        "windows-2013-32",
        "Windows (i686) (VS 2013)",
        "windows-64-vs2013-compile",
        ["release-compile", "debug-compile-rdtscp", "debug-compile-sspi-winssl", ".authentication-tests .winssl"],
        {"CC": "Visual Studio 12 2013"},
    ),
    Variant(
        "mingw-windows2016",
        "MinGW-W64 (Windows Server 2016)",
        "windows-vsCurrent-large",
        ["debug-compile-nosasl-nossl", ".latest .nossl .nosasl .server"],
        {"CC": "mingw"},
    ),
    Variant("mingw", "MinGW-W64", "windows-vsCurrent-large", ["debug-compile-no-align"], {"CC": "mingw"}),
    Variant(
        "power8-rhel81",
        "Power8 (ppc64le) (RHEL 8.1)",
        "rhel81-power8-test",
        [
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-sasl-openssl",
            ".latest .nossl",
            "test-dns-openssl",
        ],
        {"CC": "gcc"},
        batchtime=days(1),
    ),
    Variant(
        "arm-ubuntu1804",
        "*ARM (aarch64) (Ubuntu 18.04)",
        "ubuntu1804-arm64-large",
        [
            ".compression !.snappy !.zstd",
            "debug-compile-no-align",
            "release-compile",
            "debug-compile-nosasl-nossl",
            "debug-compile-nosasl-openssl",
            "debug-compile-sasl-openssl",
            ".authentication-tests .openssl",
            ".latest .nossl",
            "test-dns-openssl",
        ],
        {"CC": "gcc"},
        batchtime=days(1),
    ),
    Variant(
        "arm-ubuntu1604",
        "*ARM (aarch64) (Ubuntu 16.04)",
        "ubuntu1604-arm64-large",
        [".compression !.snappy !.zstd", "debug-compile-no-align", "release-compile"],
        {"CC": "gcc"},
        batchtime=days(1),
    ),
    Variant(
        "zseries-rhel83",
        "*zSeries",
        "rhel83-zseries-small",
        [
            "release-compile",
            #      '.compression', --> TODO: waiting on ticket CDRIVER-3258
            "debug-compile-no-align",
            "debug-compile-nosasl-nossl",
            "debug-compile-nosasl-openssl",
            "debug-compile-sasl-openssl",
            ".authentication-tests .openssl",
            ".latest .nossl",
        ],
        {"CC": "gcc"},
        batchtime=days(1),
    ),
    Variant(
        "clang60ubuntu",
        "clang 6.0 (Ubuntu 18.04)",
        "ubuntu1804-test",
        [
            "debug-compile-sasl-openssl-static",
            ".authentication-tests .asan",
        ],
        {"CC": "clang"},
    ),
    # Run AWS tests for MongoDB 4.4 and 5.0 on Ubuntu 20.04. AWS setup scripts expect Ubuntu 20.04+. MongoDB 4.4 and 5.0 are not available on 22.04.
    Variant(
        "aws-ubuntu2004",
        "AWS Tests (Ubuntu 20.04)",
        "ubuntu2004-small",
        [
            "debug-compile-aws",
            ".test-aws .4.4",
            ".test-aws .5.0",
        ],
        {"CC": "clang"},
    ),
    Variant(
        "aws-ubuntu2204",
        "AWS Tests (Ubuntu 22.04)",
        "ubuntu2004-small",
        [
            "debug-compile-aws",
            ".test-aws .6.0",
            ".test-aws .7.0",
            ".test-aws .latest",
        ],
        {"CC": "clang"},
    ),
    Variant("mongohouse", "Mongohouse Test", "ubuntu2204-small", ["debug-compile-sasl-openssl", "test-mongohouse"], {}),
    Variant(
        "ocsp",
        "OCSP tests",
        "ubuntu2004-small",
        [
            OD([("name", "debug-compile-nosasl-openssl")]),
            OD([("name", "debug-compile-nosasl-openssl-static")]),
            OD([("name", "debug-compile-nosasl-darwinssl"), ("distros", ["macos-1100"])]),
            OD([("name", "debug-compile-nosasl-winssl"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", ".ocsp-openssl")]),
            OD([("name", ".ocsp-darwinssl"), ("distros", ["macos-1100"])]),
            OD([("name", ".ocsp-winssl"), ("distros", ["windows-vsCurrent-large"])]),
            OD([("name", "debug-compile-nosasl-openssl-1.0.1")]),
            OD([("name", ".ocsp-openssl-1.0.1")]),
        ],
        {},
        batchtime=days(7),
        display_tasks=[
            {
                "name": "ocsp-openssl",
                "execution_tasks": [".ocsp-openssl"],
            },
            {
                "name": "ocsp-darwinssl",
                "execution_tasks": [".ocsp-darwinssl"],
            },
            {
                "name": "ocsp-winssl",
                "execution_tasks": [".ocsp-winssl"],
            },
            {
                "name": "ocsp-openssl-1.0.1",
                "execution_tasks": [".ocsp-openssl-1.0.1"],
            },
        ]
    ),
    Variant(
        "packaging",
        "Linux Distro Packaging",
        "debian12-latest-small",
        [
            "debian-package-build",
            OD([("name", "rpm-package-build"), ("distros", ["rhel90-arm64-small"])]),
        ],
        {},
        tags=["pr-merge-gate"],
    ),
    Variant(
        "versioned-api-ubuntu1804",
        "Versioned API Tests (Ubuntu 18.04)",
        "ubuntu1804-test",
        [
            "debug-compile-nosasl-openssl",
            "debug-compile-nosasl-nossl",
            ".versioned-api .5.0",
            ".versioned-api .6.0",
        ],
        {},
    ),
    # Test 7.0+ with Ubuntu 20.04+ since MongoDB 7.0 no longer ships binaries for Ubuntu 18.04.
    Variant(
        "versioned-api-ubuntu2004",
        "Versioned API Tests (Ubuntu 20.04)",
        "ubuntu2004-test",
        [
            "debug-compile-nosasl-openssl",
            "debug-compile-nosasl-nossl",
            ".versioned-api .7.0",
        ],
        {},
    ),
]
