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
from itertools import chain
from typing import ClassVar, Iterable, Literal, Mapping, MutableMapping, MutableSequence, Optional, Sequence

from evergreen_config_generator import Value, Scalar
from evergreen_config_generator.functions import func, s3_put
from evergreen_config_generator.tasks import (
    both_or_neither,
    MatrixTask,
    NamedTask,
    prohibit,
    require,
    Task,
    DependencySpec,
)
from evergreen_config_lib import shell_mongoc
from pkg_resources import parse_version


ToggleStr = Literal["OFF", "ON"]
OptToggleStr = Optional[ToggleStr]
TopologyStr = Literal["server"]


class CompileTask(NamedTask):
    cls_compile_sh_env: ClassVar[Mapping[str, str]] = {}
    cls_tags: ClassVar[Sequence[str]] = ()
    cls_sanitize: ClassVar[Sequence[str]] = ()

    def __init__(
        self,
        task_name: str,
        tags: Iterable[str] = (),
        config: str = "debug",
        compression: str | None = "default",
        suffix_commands: Iterable[Value] = (),
        depends_on: Iterable[DependencySpec] = (),
        prefix_commands: Iterable[Value] = (),
        sanitize: Iterable[Literal["undefined", "address", "thread"]] = (),
        *,
        CFLAGS: str | None = None,
        LDFLAGS: str | None = None,
        EXTRA_CONFIGURE_FLAGS: str | None = None,
        SSL: Literal["WINDOWS", "DARWIN", "OPENSSL", "OPENSSL_STATIC", "LIBRESSL", "OFF", None] = None,
        ENABLE_SHM_COUNTERS: OptToggleStr = None,
        CHECK_LOG: OptToggleStr = None,
        TRACING: OptToggleStr = None,
        SASL: Literal[None, "OFF", "AUTO", "CYRUS", "SSPI"] = None,
        ENABLE_RDTSCP: OptToggleStr = None,
        SRV: OptToggleStr = None,
    ):
        super(CompileTask, self).__init__(task_name=task_name, depends_on=depends_on, tags=tags)

        self.suffix_commands = list(suffix_commands)
        self.prefix_commands = list(prefix_commands)

        # Environment variables for .evergreen/scripts/compile.sh.
        self.compile_sh_opt: dict[str, str] = {}
        if config == "debug":
            self.compile_sh_opt["DEBUG"] = "ON"
        else:
            assert config == "release"
            self.compile_sh_opt["RELEASE"] = "ON"

        if CFLAGS:
            self.compile_sh_opt["CFLAGS"] = CFLAGS
        if LDFLAGS:
            self.compile_sh_opt["LDFLAGS"] = LDFLAGS
        if EXTRA_CONFIGURE_FLAGS:
            self.compile_sh_opt["EXTRA_CONFIGURE_FLAGS"] = EXTRA_CONFIGURE_FLAGS
        if SSL:
            self.compile_sh_opt["SSL"] = SSL
        if ENABLE_SHM_COUNTERS:
            self.compile_sh_opt["ENABLE_SHM_COUNTERS"] = ENABLE_SHM_COUNTERS
        if CHECK_LOG:
            self.compile_sh_opt["CHECK_LOG"] = CHECK_LOG
        if TRACING:
            self.compile_sh_opt["TRACING"] = TRACING
        if SASL:
            self.compile_sh_opt["SASL"] = SASL
        if ENABLE_RDTSCP:
            self.compile_sh_opt["ENABLE_RDTSCP"] = ENABLE_RDTSCP
        if SRV:
            self.compile_sh_opt["SRV"] = SRV

        if compression != "default":
            self.compile_sh_opt["SNAPPY"] = "ON" if compression in ("all", "snappy") else "OFF"
            self.compile_sh_opt["ZLIB"] = "BUNDLED" if compression in ("all", "zlib") else "OFF"
            self.compile_sh_opt["ZSTD"] = "ON" if compression in ("all", "zstd") else "OFF"

        if sanitize:
            self.compile_sh_opt["SANITIZE"] = ",".join(sanitize)

        self.compile_sh_opt.update(type(self).cls_compile_sh_env)

    def additional_script_env(self) -> Mapping[str, str]:
        return {}

    def to_dict(self):
        task = super(CompileTask, self).to_dict()
        commands = task["commands"]
        assert isinstance(commands, MutableSequence), task

        commands.extend(self.prefix_commands)

        script = "env"
        for opt, value in sorted(self.compile_sh_opt.items()):
            script += ' %s="%s"' % (opt, value)

        script += " bash .evergreen/scripts/compile.sh"
        commands.append(shell_mongoc(script, add_expansions_to_env=True))
        commands.append(func("upload-build"))
        commands.extend(self.suffix_commands)
        return task

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield from self.cls_tags


class SpecialTask(CompileTask):
    cls_tags: ClassVar[Sequence[str]] = ["special"]


class CompileWithClientSideEncryption(CompileTask):
    cls_compile_sh_env: ClassVar[Mapping[str, str]] = dict(
        # Compiling with ClientSideEncryption support requires linking against the library libmongocrypt.
        COMPILE_LIBMONGOCRYPT="ON",
        EXTRA_CONFIGURE_FLAGS="-DENABLE_PIC=ON",
    )
    cls_tags: ClassVar[Sequence[str]] = "client-side-encryption", "special"


class CompileWithClientSideEncryptionAsan(CompileTask):
    cls_compile_sh_env: ClassVar[Mapping[str, str]] = dict(
        CFLAGS="-fno-omit-frame-pointer",
        COMPILE_LIBMONGOCRYPT="ON",
        CHECK_LOG="ON",
        EXTRA_CONFIGURE_FLAGS="-DENABLE_EXTRA_ALIGNMENT=OFF",
        PATH="/usr/lib/llvm-3.8/bin:$PATH",
    )
    cls_tags: ClassVar[Sequence[str]] = ["client-side-encryption"]
    cls_sanitize: ClassVar[Sequence[str]] = ["address"]


class LinkTask(NamedTask):
    def __init__(
        self, task_name: str, suffix_commands: Iterable[Value], orchestration: Literal[True, False, "ssl"] = True
    ):
        if orchestration == "ssl":
            # Actual value of SSL does not matter here so long as it is not 'nossl'.
            bootstrap_commands = [func("fetch-det"), func("bootstrap-mongo-orchestration", SSL="openssl")]
        elif orchestration:
            bootstrap_commands = [func("fetch-det"), func("bootstrap-mongo-orchestration")]
        else:
            bootstrap_commands = []

        super().__init__(
            task_name=task_name,
            commands=bootstrap_commands + list(suffix_commands),
        )


all_tasks = [
    CompileTask(
        "hardened-compile",
        tags=["hardened"],
        compression=None,
        CFLAGS="-fno-strict-overflow -D_FORTIFY_SOURCE=2 -fstack-protector-all -fPIE -O",
        LDFLAGS="-pie -Wl,-z,relro -Wl,-z,now",
    ),
    CompileTask("debug-compile-compression-zlib", tags=["zlib", "compression"], compression="zlib"),
    CompileTask("debug-compile-compression-snappy", tags=["snappy", "compression"], compression="snappy"),
    CompileTask("debug-compile-compression-zstd", tags=["zstd", "compression"], compression="zstd"),
    CompileTask("debug-compile-compression", tags=["zlib", "snappy", "zstd", "compression"], compression="all"),
    CompileTask(
        "debug-compile-no-align",
        tags=["debug-compile"],
        compression="zlib",
        EXTRA_CONFIGURE_FLAGS="-DENABLE_EXTRA_ALIGNMENT=OFF",
    ),
    CompileTask("debug-compile-nosasl-nossl", tags=["debug-compile", "nosasl", "nossl"], SSL="OFF"),
    CompileTask("debug-compile-lto", CFLAGS="-flto"),
    CompileTask("debug-compile-lto-thin", CFLAGS="-flto=thin"),
    CompileTask("debug-compile-no-counters", tags=["debug-compile", "no-counters"], ENABLE_SHM_COUNTERS="OFF"),
    SpecialTask(
        "debug-compile-asan-clang",
        tags=["debug-compile", "asan-clang"],
        compression="zlib",
        CFLAGS="-fno-omit-frame-pointer",
        CHECK_LOG="ON",
        sanitize=["address"],
        EXTRA_CONFIGURE_FLAGS="-DENABLE_EXTRA_ALIGNMENT=OFF",
    ),
    SpecialTask(
        "debug-compile-asan-clang-openssl",
        tags=["debug-compile", "asan-clang"],
        compression="zlib",
        CFLAGS="-fno-omit-frame-pointer",
        CHECK_LOG="ON",
        sanitize=["address"],
        EXTRA_CONFIGURE_FLAGS="-DENABLE_EXTRA_ALIGNMENT=OFF",
        SSL="OPENSSL",
    ),
    CompileTask("compile-tracing", TRACING="ON", CFLAGS="-Werror -Wno-cast-align"),
    CompileTask("release-compile", config="release"),
    CompileTask("debug-compile-nosasl-openssl", tags=["debug-compile", "nosasl", "openssl"], SSL="OPENSSL"),
    CompileTask(
        "debug-compile-nosasl-openssl-static", tags=["debug-compile", "nosasl", "openssl-static"], SSL="OPENSSL_STATIC"
    ),
    CompileTask("debug-compile-nosasl-darwinssl", tags=["debug-compile", "nosasl", "darwinssl"], SSL="DARWIN"),
    CompileTask("debug-compile-nosasl-winssl", tags=["debug-compile", "nosasl", "winssl"], SSL="WINDOWS"),
    CompileTask("debug-compile-sasl-nossl", tags=["debug-compile", "sasl", "nossl"], SASL="AUTO", SSL="OFF"),
    CompileTask("debug-compile-sasl-openssl", tags=["debug-compile", "sasl", "openssl"], SASL="AUTO", SSL="OPENSSL"),
    CompileTask(
        "debug-compile-sasl-openssl-static",
        tags=["debug-compile", "sasl", "openssl-static"],
        SASL="AUTO",
        SSL="OPENSSL_STATIC",
    ),
    CompileTask("debug-compile-sasl-darwinssl", tags=["debug-compile", "sasl", "darwinssl"], SASL="AUTO", SSL="DARWIN"),
    CompileTask("debug-compile-sspi-nossl", tags=["debug-compile", "sspi", "nossl"], SASL="SSPI", SSL="OFF"),
    CompileTask("debug-compile-sspi-openssl", tags=["debug-compile", "sspi", "openssl"], SASL="SSPI", SSL="OPENSSL"),
    CompileTask(
        "debug-compile-sspi-openssl-static",
        tags=["debug-compile", "sspi", "openssl-static"],
        SASL="SSPI",
        SSL="OPENSSL_STATIC",
    ),
    CompileTask("debug-compile-rdtscp", ENABLE_RDTSCP="ON"),
    CompileTask("debug-compile-sspi-winssl", tags=["debug-compile", "sspi", "winssl"], SASL="SSPI", SSL="WINDOWS"),
    CompileTask("debug-compile-nosrv", tags=["debug-compile"], SRV="OFF"),
    LinkTask("link-with-cmake", suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1)]),
    LinkTask(
        "link-with-cmake-ssl",
        suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1, ENABLE_SSL=1)],
    ),
    LinkTask(
        "link-with-cmake-snappy",
        suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1, ENABLE_SNAPPY="ON")],
    ),
    LinkTask("link-with-cmake-mac", suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1)]),
    LinkTask(
        "link-with-cmake-deprecated",
        suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1, BUILD_SAMPLE_WITH_CMAKE_DEPRECATED=1)],
    ),
    LinkTask(
        "link-with-cmake-ssl-deprecated",
        suffix_commands=[
            func(
                "link sample program",
                BUILD_SAMPLE_WITH_CMAKE=1,
                BUILD_SAMPLE_WITH_CMAKE_DEPRECATED=1,
                ENABLE_SSL=1,
            )
        ],
    ),
    LinkTask(
        "link-with-cmake-snappy-deprecated",
        suffix_commands=[
            func(
                "link sample program",
                BUILD_SAMPLE_WITH_CMAKE=1,
                BUILD_SAMPLE_WITH_CMAKE_DEPRECATED=1,
                ENABLE_SNAPPY="ON",
            )
        ],
    ),
    LinkTask(
        "link-with-cmake-mac-deprecated",
        suffix_commands=[func("link sample program", BUILD_SAMPLE_WITH_CMAKE=1, BUILD_SAMPLE_WITH_CMAKE_DEPRECATED=1)],
    ),
    LinkTask("link-with-cmake-windows", suffix_commands=[func("link sample program MSVC")]),
    LinkTask(
        "link-with-cmake-windows-ssl",
        suffix_commands=[func("link sample program MSVC", ENABLE_SSL=1)],
        orchestration="ssl",
    ),
    LinkTask("link-with-cmake-windows-snappy", suffix_commands=[func("link sample program MSVC", ENABLE_SNAPPY="ON")]),
    LinkTask("link-with-cmake-mingw", suffix_commands=[func("link sample program mingw")]),
    LinkTask("link-with-pkg-config", suffix_commands=[func("link sample program")]),
    LinkTask("link-with-pkg-config-mac", suffix_commands=[func("link sample program")]),
    LinkTask("link-with-pkg-config-ssl", suffix_commands=[func("link sample program", ENABLE_SSL=1)]),
    LinkTask("link-with-bson", suffix_commands=[func("link sample program bson")], orchestration=False),
    LinkTask("link-with-bson-mac", suffix_commands=[func("link sample program bson")], orchestration=False),
    LinkTask("link-with-bson-windows", suffix_commands=[func("link sample program MSVC bson")], orchestration=False),
    LinkTask("link-with-bson-mingw", suffix_commands=[func("link sample program mingw bson")], orchestration=False),
    NamedTask(
        "debian-package-build",
        commands=[
            shell_mongoc('export IS_PATCH="${is_patch}"\n' "sh .evergreen/scripts/debian_package_build.sh"),
            s3_put(
                local_file="deb.tar.gz",
                remote_file="${branch_name}/mongo-c-driver-debian-packages-${CURRENT_VERSION}.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
            s3_put(
                local_file="deb.tar.gz",
                remote_file="${branch_name}/${revision}/${version_id}/${build_id}/${execution}/mongo-c-driver-debian-packages.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
            s3_put(
                local_file="deb-i386.tar.gz",
                remote_file="${branch_name}/mongo-c-driver-debian-packages-i386-${CURRENT_VERSION}.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
            s3_put(
                local_file="deb-i386.tar.gz",
                remote_file="${branch_name}/${revision}/${version_id}/${build_id}/${execution}/mongo-c-driver-debian-packages-i386.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
        ],
    ),
    NamedTask(
        "rpm-package-build",
        commands=[
            shell_mongoc('export IS_PATCH="${is_patch}"\n' "sh .evergreen/scripts/check_rpm_spec.sh"),
            shell_mongoc("sh .evergreen/scripts/build_snapshot_rpm.sh"),
            s3_put(
                local_file="rpm.tar.gz",
                remote_file="${branch_name}/mongo-c-driver-rpm-packages-${CURRENT_VERSION}.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
            s3_put(
                local_file="rpm.tar.gz",
                remote_file="${branch_name}/${revision}/${version_id}/${build_id}/${execution}/mongo-c-driver-rpm-packages.tar.gz",
                content_type="${content_type|application/x-gzip}",
            ),
            shell_mongoc("sudo rm -rf ../build ../mock-result ../rpm.tar.gz\n" "export MOCK_TARGET_CONFIG=rocky+epel-9-aarch64\n" "sh .evergreen/scripts/build_snapshot_rpm.sh"),
            shell_mongoc("sudo rm -rf ../build ../mock-result ../rpm.tar.gz\n" "export MOCK_TARGET_CONFIG=rocky+epel-8-aarch64\n" "sh .evergreen/scripts/build_snapshot_rpm.sh"),
        ],
    ),
    NamedTask(
        "install-uninstall-check-mingw",
        commands=[
            shell_mongoc(
                r"""
                . .evergreen/scripts/find-cmake-latest.sh
                export CMAKE="$(find_cmake_latest)"
                export CC="C:/mingw-w64/x86_64-4.9.1-posix-seh-rt_v3-rev1/mingw64/bin/gcc.exe"
                BSON_ONLY=1 cmd.exe /c .\\.evergreen\\scripts\\install-uninstall-check-windows.cmd
                cmd.exe /c .\\.evergreen\\scripts\\install-uninstall-check-windows.cmd""",
                include_expansions_in_env=["distro_id"],
            )
        ],
    ),
    NamedTask(
        "install-uninstall-check-msvc",
        commands=[
            shell_mongoc(
                r"""
                . .evergreen/scripts/find-cmake-latest.sh
                export CMAKE="$(find_cmake_latest)"
                export CC="Visual Studio 14 2015 Win64"
                BSON_ONLY=1 cmd.exe /c .\\.evergreen\\scripts\\install-uninstall-check-windows.cmd
                cmd.exe /c .\\.evergreen\\scripts\\install-uninstall-check-windows.cmd""",
                include_expansions_in_env=["distro_id"],
            )
        ],
    ),
    NamedTask(
        "install-uninstall-check",
        commands=[
            shell_mongoc(
                r"""
                . .evergreen/scripts/find-cmake-latest.sh
                export CMAKE="$(find_cmake_latest)"
                DESTDIR="$(pwd)/dest" bash ./.evergreen/scripts/install-uninstall-check.sh
                BSON_ONLY=1 bash ./.evergreen/scripts/install-uninstall-check.sh
                bash ./.evergreen/scripts/install-uninstall-check.sh""",
                include_expansions_in_env=["distro_id"],
            )
        ],
    ),
    CompileTask("debug-compile-with-warnings", CFLAGS="-Werror -Wno-cast-align"),
    CompileWithClientSideEncryption(
        "debug-compile-sasl-openssl-static-cse",
        tags=["debug-compile", "sasl", "openssl-static"],
        SASL="AUTO",
        SSL="OPENSSL_STATIC",
    ),
    CompileTask(
        "debug-compile-nosasl-openssl-1.0.1",
        prefix_commands=[func("install ssl", SSL="openssl-1.0.1u")],
        CFLAGS="-Wno-redundant-decls",
        SSL="OPENSSL",
        SASL="OFF",
    ),
    NamedTask(
        "build-and-test-with-toolchain",
        commands=[
            OD(
                [
                    ("command", "s3.get"),
                    (
                        "params",
                        OD(
                            [
                                ("aws_key", "${aws_key}"),
                                ("aws_secret", "${aws_secret}"),
                                ("remote_file", "mongo-c-toolchain/${distro_id}/2023/06/07/mongo-c-toolchain.tar.gz"),
                                ("bucket", "mongo-c-toolchain"),
                                ("local_file", "mongo-c-toolchain.tar.gz"),
                            ]
                        ),
                    ),
                ]
            ),
            shell_mongoc("bash ./.evergreen/scripts/build-and-test-with-toolchain.sh"),
        ],
    ),
    NamedTask(
        "install-libmongoc-after-libbson",
        commands=[shell_mongoc("bash ./.evergreen/scripts/install-libmongoc-after-libbson.sh"),],
    ),
]


class CoverageTask(MatrixTask):
    axes = OD(
        [
            ("version", ["latest"]),
            ("topology", ["replica_set"]),
            ("auth", [True]),
            ("sasl", ["sasl"]),
            ("ssl", ["openssl"]),
            ("cse", [False, True]),
        ]
    )

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield "test-coverage"
        yield str(self.settings.version)
        if self.cse:
            yield "client-side-encryption"

    def name_parts(self) -> Iterable[str]:
        yield "test-coverage"
        yield self.display("version")
        yield self.display("topology").replace("_", "-")
        yield from map(self.display, ("auth", "sasl", "ssl"))
        if self.settings.cse:
            yield "cse"

    @property
    def cse(self) -> bool:
        return bool(self.settings.cse)

    def post_commands(self) -> Iterable[Value]:
        if self.cse:
            yield func(
                "compile coverage",
                SASL="AUTO",
                SSL="OPENSSL",
                COMPILE_LIBMONGOCRYPT="ON",
                EXTRA_CONFIGURE_FLAGS='EXTRA_CONFIGURE_FLAGS="-DENABLE_PIC=ON"',
            )
        else:
            yield func("compile coverage", SASL="AUTO", SSL="OPENSSL")

        yield func("fetch-det")
        yield func(
            "bootstrap-mongo-orchestration",
            MONGODB_VERSION=self.settings.version,
            TOPOLOGY=self.settings.topology,
            AUTH=self.display("auth"),
            SSL=self.display("ssl"),
        )
        yield func("run-simple-http-server")
        extra = {"COVERAGE": "ON"}
        if self.cse:
            extra["CLIENT_SIDE_ENCRYPTION"] = "ON"
            yield func("run-mock-kms-servers")
        yield func("run-tests", AUTH=self.display("auth"), SSL=self.display("ssl"), **extra)
        yield func("upload coverage")
        yield func("update codecov.io")

    def do_is_valid_combination(self) -> bool:
        # Limit coverage tests to test-coverage-latest-replica-set-auth-sasl-openssl (+ cse).
        require(self.setting_eq("topology", "replica_set"))
        require(self.setting_eq("sasl", "sasl"))
        require(self.setting_eq("ssl", "openssl"))
        require(self.setting_eq("version", "latest"))
        require(self.settings.auth is True)

        if not self.cse:
            # No further requirements
            return True

        # CSE has extra requirements
        if self.settings.version != "latest":
            # We only work with 4.2 or newer for CSE
            require(parse_version(str(self.settings.version)) >= parse_version("4.2"))
        return True


all_tasks = chain(all_tasks, CoverageTask.matrix())


class DNSTask(MatrixTask):
    axes = OD(
        [
            ("auth", [False, True]),
            ("loadbalanced", [False, True]),
            ("ssl", ["openssl", "winssl", "darwinssl"]),
        ]
    )

    name_prefix = "test-dns"

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield self.build_task_name

    @property
    def build_task_name(self) -> str:
        sasl = "sspi" if self.settings.ssl == "winssl" else "sasl"
        return f'debug-compile-{sasl}-{self.display("ssl")}'

    def name_parts(self) -> Iterable[str]:
        yield "test-dns"
        if self.settings.auth:
            yield "auth"
        if self.settings.loadbalanced:
            yield "loadbalanced"
        yield self.display("ssl")

    def post_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self.build_task_name)
        yield func("fetch-det")
        if self.settings.loadbalanced:
            orchestration = func(
                "bootstrap-mongo-orchestration",
                TOPOLOGY="sharded_cluster",
                AUTH="auth" if self.settings.auth else "noauth",
                SSL="ssl",
                LOAD_BALANCER="on",
            )
        else:
            orchestration = func(
                "bootstrap-mongo-orchestration",
                TOPOLOGY="replica_set",
                AUTH="auth" if self.settings.auth else "noauth",
                SSL="ssl",
            )

        if self.settings.auth:
            vars = orchestration["vars"]
            assert isinstance(vars, MutableMapping)
            vars["AUTHSOURCE"] = "thisDB"

        yield orchestration

        dns = "on"
        if self.settings.loadbalanced:
            dns = "loadbalanced"
            yield func("fetch-det")
            yield func("start-load-balancer", MONGODB_URI="mongodb://localhost:27017,localhost:27018")
        elif self.settings.auth:
            dns = "dns-auth"
        yield func("run-tests", SSL="ssl", AUTH=self.display("auth"), DNS=dns)

    def do_is_valid_combination(self) -> bool:
        prohibit(bool(self.settings.loadbalanced) and bool(self.settings.auth))
        # Load balancer tests only run on some Linux hosts in Evergreen until CDRIVER-4041 is resolved.
        prohibit(bool(self.settings.loadbalanced) and self.settings.ssl in ["darwinssl", "winssl"])
        return True


all_tasks = chain(all_tasks, DNSTask.matrix())


class CompressionTask(MatrixTask):
    axes = OD([("compression", ["zlib", "snappy", "zstd", "compression"])])
    name_prefix = "test-latest-server"

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield self.build_task_name

    @property
    def build_task_name(self) -> str:
        return f"debug-compile-{self._compressor_suffix()}"

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield "compression"
        yield "latest"
        yield from self._compressor_list()

    def name_parts(self) -> Iterable[str]:
        return [self.name_prefix, self._compressor_suffix()]

    def post_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self.build_task_name)
        yield func("fetch-det")
        if self.settings.compression == "compression":
            orc_file = "snappy-zlib-zstd"
        else:
            orc_file = self.settings.compression
        yield func("bootstrap-mongo-orchestration", AUTH="noauth", SSL="nossl", ORCHESTRATION_FILE=orc_file)
        yield func("run-simple-http-server")
        yield func("run-tests", AUTH="noauth", SSL="nossl", COMPRESSORS=",".join(self._compressor_list()))

    def _compressor_suffix(self):
        if self.settings.compression == "zlib":
            return "compression-zlib"
        elif self.settings.compression == "snappy":
            return "compression-snappy"
        elif self.settings.compression == "zstd":
            return "compression-zstd"
        else:
            return "compression"

    def _compressor_list(self):
        if self.settings.compression == "zlib":
            return ["zlib"]
        elif self.settings.compression == "snappy":
            return ["snappy"]
        elif self.settings.compression == "zstd":
            return ["zstd"]
        else:
            return ["snappy", "zlib", "zstd"]


all_tasks = chain(all_tasks, CompressionTask.matrix())


class SpecialIntegrationTask(NamedTask):
    def __init__(
        self,
        task_name: str,
        main_dep: str = "debug-compile-sasl-openssl",
        uri: str | None = None,
        tags: Iterable[str] = (),
        version: str = "latest",
        topology: str = "server",
    ):
        self._main_dep = main_dep
        super().__init__(task_name, depends_on=[self._main_dep], tags=tags)
        self._uri = uri
        self._version = version
        self._topo = topology

    def pre_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self._main_dep)
        yield func("fetch-det")
        yield func("bootstrap-mongo-orchestration", MONGODB_VERSION=self._version, TOPOLOGY=self._topo)
        yield func("run-simple-http-server")
        yield func("run-tests", URI=self._uri)


all_tasks = chain(
    all_tasks,
    [
        # Verify that retryWrites=true is ignored with standalone.
        SpecialIntegrationTask("retry-true-latest-server", uri="mongodb://localhost/?retryWrites=true"),
        SpecialIntegrationTask("test-latest-server-hardened", "hardened-compile", tags=["hardened", "latest"]),
    ],
)


class AuthTask(MatrixTask):
    axes = OD([("sasl", ["sasl", "sspi", False]), ("ssl", ["openssl", "darwinssl", "winssl"])])

    name_prefix = "authentication-tests"

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield "authentication-tests"
        yield self.display("ssl")
        yield self.display("sasl")

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield self.build_task_name

    def post_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self.build_task_name)
        yield func("prepare-kerberos")
        yield func("run auth tests")

    @property
    def build_task_name(self) -> str:
        return f'debug-compile-{self.display("sasl")}-{self.display("ssl")}'

    def name_parts(self) -> Iterable[str]:
        yield self.name_prefix
        yield self.display("ssl")
        if not self.settings.sasl:
            yield "nosasl"

    def do_is_valid_combination(self) -> bool:
        both_or_neither(self.settings.ssl == "winssl", self.settings.sasl == "sspi")
        if not self.settings.sasl:
            require(self.settings.ssl == "openssl")
        return True


all_tasks = chain(all_tasks, AuthTask.matrix())


class PostCompileTask(NamedTask):
    def __init__(self, name: str, tags: Iterable[str], get_build: str, commands: Iterable[Value]):
        super().__init__(name, commands=commands, tags=tags, depends_on=[get_build])
        self._dep = get_build

    def pre_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self._dep)


all_tasks = chain(
    all_tasks,
    [
        PostCompileTask(
            "test-mongohouse",
            tags=[],
            get_build="debug-compile-sasl-openssl",
            commands=[func("fetch-det"), func("build mongohouse"), func("run mongohouse"), func("test mongohouse")],
        ),
        NamedTask(
            "authentication-tests-asan-memcheck",
            tags=["authentication-tests", "asan"],
            commands=[
                shell_mongoc(
                    """
            env SANITIZE=address DEBUG=ON SASL=AUTO SSL=OPENSSL EXTRA_CONFIGURE_FLAGS='-DENABLE_EXTRA_ALIGNMENT=OFF' bash .evergreen/scripts/compile.sh
            """,
                    add_expansions_to_env=True,
                ),
                func("prepare-kerberos"),
                func("run auth tests", ASAN="on"),
            ],
        )
    ],
)

# Add API version tasks.
for server_version in [ "7.0", "6.0", "5.0"]:
    all_tasks = chain(
        all_tasks,
        [
            PostCompileTask(
                "test-versioned-api-" + server_version,
                tags=["versioned-api", f"{server_version}"],
                get_build="debug-compile-nosasl-openssl",
                commands=[
                    func("fetch-det"),
                    func(
                        "bootstrap-mongo-orchestration",
                        TOPOLOGY="server",
                        AUTH="auth",
                        SSL="ssl",
                        MONGODB_VERSION=server_version,
                        REQUIRE_API_VERSION="true",
                    ),
                    func("run-simple-http-server"),
                    func("run-tests", MONGODB_API_VERSION=1, AUTH="auth", SSL="ssl"),
                ],
            ),
            PostCompileTask(
                "test-versioned-api-accept-version-two-" + server_version,
                tags=["versioned-api", f"{server_version}"],
                get_build="debug-compile-nosasl-nossl",
                commands=[
                    func("fetch-det"),
                    func(
                        "bootstrap-mongo-orchestration",
                        TOPOLOGY="server",
                        AUTH="noauth",
                        SSL="nossl",
                        MONGODB_VERSION=server_version,
                        ORCHESTRATION_FILE="versioned-api-testing",
                    ),
                    func("run-simple-http-server"),
                    func("run-tests", MONGODB_API_VERSION=1, AUTH="noauth", SSL="nossl"),
                ],
            )
        ]
    )


class SSLTask(Task):
    def __init__(
        self,
        version: str,
        patch: str,
        cflags: str = "",
        fips: bool = False,
        enable_ssl: str | Literal[False] = False,
        test_params: Mapping[str, Scalar] | None = None,
    ):
        full_version = version + patch + ("-fips" if fips else "")
        self.enable_ssl = enable_ssl
        script = "env"
        if cflags:
            script += f" CFLAGS={cflags}"

        script += " DEBUG=ON SASL=OFF"

        if enable_ssl is not False:
            script += " SSL=" + enable_ssl
        elif "libressl" in version:
            script += " SSL=LIBRESSL"
        else:
            script += " SSL=OPENSSL"

        script += " bash .evergreen/scripts/compile.sh"

        super(SSLTask, self).__init__(
            commands=[
                func("install ssl", SSL=full_version),
                shell_mongoc(script, add_expansions_to_env=True),
                func("run auth tests", **(test_params or {})),
                func("upload-build"),
            ]
        )

        self.version = version
        self.fips = fips

    @property
    def name(self):
        s = "build-and-run-authentication-tests-" + self.version
        if self.fips:
            return s + "-fips"
        if self.enable_ssl is not False:
            return s + "-" + str(self.enable_ssl).lower()

        return s


all_tasks = chain(
    all_tasks,
    [
        SSLTask(
            "openssl-1.0.1",
            "u",
            cflags="-Wno-redundant-decls",
        ),
        SSLTask("openssl-1.0.1", "u", cflags="-Wno-redundant-decls", fips=True),
        SSLTask(
            "openssl-1.0.2",
            "l",
            cflags="-Wno-redundant-decls",
        ),
        SSLTask("openssl-1.1.0", "l"),
        SSLTask("libressl-2.5", ".2", test_params=dict(require_tls12=True)),
        SSLTask("libressl-3.0", ".2", enable_ssl="AUTO", test_params=dict(require_tls12=True)),
        SSLTask("libressl-3.0", ".2", test_params=dict(require_tls12=True)),
    ],
)


class IPTask(MatrixTask):
    axes = OD(
        [
            ("client", ["ipv6", "ipv4", "localhost"]),
            ("server", ["ipv6", "ipv4"]),
        ]
    )

    name_prefix = "test-latest"

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield "debug-compile-nosasl-nossl"

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield from ("nossl", "nosasl", "server", "ipv4-ipv6", "latest")

    def post_commands(self) -> Iterable[Value]:
        return [
            func("fetch-build", BUILD_NAME="debug-compile-nosasl-nossl"),
            func("fetch-det"),
            func("bootstrap-mongo-orchestration", IPV4_ONLY=self.on_off("server", "ipv4")),
            func("run-simple-http-server"),
            func(
                "run-tests",
                IPV4_ONLY=self.on_off("server", "ipv4"),
                URI={
                    "ipv6": "mongodb://[::1]/",
                    "ipv4": "mongodb://127.0.0.1/",
                    "localhost": "mongodb://localhost/",
                }[str(self.settings.client)],
            ),
        ]

    def name_parts(self) -> Iterable[str]:
        return (
            self.name_prefix,
            f'server-{self.display("server")}',
            f'client-{self.display("client")}',
            "noauth",
            "nosasl",
            "nossl",
        )

    def do_is_valid_combination(self) -> bool:
        # This would fail by design.
        if self.settings.server == "ipv4":
            prohibit(self.settings.client == "ipv6")

        # Default configuration is tested in other variants.
        if self.settings.server == "ipv6":
            prohibit(self.settings.client == "localhost")
        return True


all_tasks = chain(all_tasks, IPTask.matrix())

aws_compile_task = NamedTask(
    "debug-compile-aws",
    commands=[
        shell_mongoc(
            """
            export distro_id='${distro_id}' # Required by find_cmake_latest.
            . .evergreen/scripts/find-cmake-latest.sh
            cmake_binary="$(find_cmake_latest)"

            # Allow reuse of ccache compilation results between different build directories.
            export CCACHE_BASEDIR CCACHE_NOHASHDIR
            CCACHE_BASEDIR="$(pwd)"
            CCACHE_NOHASHDIR=1

            # Compile test-awsauth. Disable unnecessary dependencies since test-awsauth is copied to a remote Ubuntu 20.04 ECS cluster for testing, which may not have all dependent libraries.
            export CC='${CC}'
            "$cmake_binary" -DENABLE_TRACING=ON -DENABLE_SASL=OFF -DENABLE_SNAPPY=OFF -DENABLE_ZSTD=OFF -DENABLE_CLIENT_SIDE_ENCRYPTION=OFF .
            "$cmake_binary" --build . --target test-awsauth
            """
        ),
        func("upload-build"),
    ],
)

all_tasks = chain(all_tasks, [aws_compile_task])


class AWSTestTask(MatrixTask):
    axes = OD(
        [
            ("testcase", ["regular", "ec2", "ecs", "lambda", "assume_role", "assume_role_with_web_identity"]),
            ("version", ["latest", "7.0", "6.0", "5.0", "4.4"]),
        ]
    )

    name_prefix = "test-aws-openssl"

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield "debug-compile-aws"

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield f'{self.settings.version}'
        yield f'test-aws'

    def post_commands(self) -> Iterable[Value]:
        return [
            func("fetch-build", BUILD_NAME="debug-compile-aws"),
            func("fetch-det"),
            func(
                "bootstrap-mongo-orchestration",
                AUTH="auth",
                ORCHESTRATION_FILE="auth-aws",
                MONGODB_VERSION=self.settings.version,
                TOPOLOGY="server",
            ),
            func("run aws tests", TESTCASE=str(self.settings.testcase).upper()),
        ]

    @property
    def name(self):
        return f"{self.name_prefix}-{self.settings.testcase}-{self.settings.version}"


all_tasks = chain(all_tasks, AWSTestTask.matrix())


class OCSPTask(MatrixTask):
    axes = OD(
        [
            (
                "test",
                [
                    "test_1",
                    "test_2",
                    "test_3",
                    "test_4",
                    "soft_fail_test",
                    "malicious_server_test_1",
                    "malicious_server_test_2",
                    "cache",
                ],
            ),
            ("delegate", ["delegate", "nodelegate"]),
            ("cert", ["rsa", "ecdsa"]),
            ("ssl", ["openssl", "openssl-1.0.1", "darwinssl", "winssl"]),
            ("version", ["latest", "7.0", "6.0", "5.0", "4.4"]),
        ]
    )

    name_prefix = "test-ocsp"

    @property
    def build_task_name(self) -> str:
        return f'debug-compile-nosasl-{self.display("ssl")}'

    def additional_tags(self) -> Iterable[str]:
        yield from super().additional_tags()
        yield f'ocsp-{self.display("ssl")}'

    def additional_dependencies(self) -> Iterable[DependencySpec]:
        yield self.build_task_name

    @property
    def name(self):
        return f"ocsp-{self.settings.ssl}-{self.test}-{self.settings.cert}-{self.settings.delegate}-{self.settings.version}"

    @property
    def test(self) -> str:
        return str(self.settings.test)

    def post_commands(self) -> Iterable[Value]:
        yield func("fetch-build", BUILD_NAME=self.build_task_name)
        yield func("fetch-det")

        stapling = "mustStaple"
        if self.test in ["test_3", "test_4", "soft_fail_test", "cache"]:
            stapling = "disableStapling"
        if self.test in ["malicious_server_test_1", "malicious_server_test_2"]:
            stapling = "mustStaple-disableStapling"

        orchestration_file = "%s-basic-tls-ocsp-%s" % (self.settings.cert, stapling)
        orchestration = func(
            "bootstrap-mongo-orchestration",
            MONGODB_VERSION=self.settings.version,
            TOPOLOGY="server",
            SSL="ssl",
            OCSP="on",
            ORCHESTRATION_FILE=orchestration_file,
        )

        # The cache test expects a revoked response from an OCSP responder, exactly like TEST_4.
        test_column = "TEST_4" if self.test == "cache" else str(self.test).upper()
        use_delegate = "ON" if self.settings.delegate == "delegate" else "OFF"

        yield (
            shell_mongoc(
                f"""
                TEST_COLUMN={test_column} CERT_TYPE={self.settings.cert} USE_DELEGATE={use_delegate} bash .evergreen/scripts/run-ocsp-responder.sh
                """
            )
        )

        yield (orchestration)

        if self.build_task_name == "debug-compile-nosasl-openssl-1.0.1":
            # LD_LIBRARY_PATH is needed so the in-tree OpenSSL 1.0.1 is found at runtime
            if self.test == "cache":
                yield (
                    shell_mongoc(
                        f"""
                        LD_LIBRARY_PATH=$(pwd)/install-dir/lib CERT_TYPE={self.settings.cert} bash .evergreen/scripts/run-ocsp-cache-test.sh
                        """
                    )
                )
            else:
                yield (
                    shell_mongoc(
                        f"""
                        LD_LIBRARY_PATH=$(pwd)/install-dir/lib TEST_COLUMN={self.test.upper()} CERT_TYPE={self.settings.cert} bash .evergreen/scripts/run-ocsp-test.sh
                        """
                    )
                )
        else:
            if self.test == "cache":
                yield (
                    shell_mongoc(
                        f"""
                        CERT_TYPE={self.settings.cert} bash .evergreen/scripts/run-ocsp-cache-test.sh
                        """
                    )
                )
            else:
                yield (
                    shell_mongoc(
                        f"""
                        TEST_COLUMN={self.test.upper()} CERT_TYPE={self.settings.cert} bash .evergreen/scripts/run-ocsp-test.sh
                        """
                    )
                )

    def to_dict(self):
        task = super(MatrixTask, self).to_dict()

        # OCSP tests should run with a batchtime of 14 days. Avoid running OCSP
        # tests in patch builds by default (only in commit builds).
        task["patchable"] = False

        return task

    # Testing in OCSP has a lot of exceptions.
    def do_is_valid_combination(self) -> bool:
        if self.settings.ssl == "darwinssl":
            # Secure Transport quietly ignores a must-staple certificate with no stapled response.
            prohibit(self.test == "malicious_server_test_2")

        # ECDSA certs can't be loaded (in the PEM format they're stored) on Windows/macOS. Skip them.
        if self.settings.ssl == "darwinssl" or self.settings.ssl == "winssl":
            prohibit(self.settings.cert == "ecdsa")

        # OCSP stapling is not supported on macOS or Windows.
        if self.settings.ssl == "darwinssl" or self.settings.ssl == "winssl":
            prohibit(self.test in ["test_1", "test_2", "cache"])

        if self.test == "soft_fail_test" or self.test == "malicious_server_test_2" or self.test == "cache":
            prohibit(self.settings.delegate == "delegate")
        return True


all_tasks = chain(all_tasks, OCSPTask.matrix())

all_tasks = list(all_tasks)
