from __future__ import annotations

import functools
import re
from typing import Iterable, Literal, Mapping, NamedTuple, TypeVar

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import BuiltInCommand, EvgCommandType, subprocess_exec
from shrub.v3.evg_task import EvgTaskRef

from ..etc.utils import Task, all_possible

T = TypeVar("T")

_ENV_PARAM_NAME = "MONGOC_EARTHLY_ENV"
_CC_PARAM_NAME = "MONGOC_EARTHLY_C_COMPILER"
"The name of the EVG expansion for the Earthly c_compiler argument"


EnvKey = Literal[
    "u18",
    "u20",
    "u22",
    "alpine3.16",
    "alpine3.17",
    "alpine3.18",
    "alpine3.19",
    "archlinux",
]
"Identifiers for environments. These correspond to special 'env.*' targets in the Earthfile."
CompilerName = Literal["gcc", "clang"]
"The name of the compiler program that is used for the build. Passed via --c_compiler to Earthly."

# Other options: SSPI (Windows only), AUTO (not reliably test-able without more environments)
SASLOption = Literal["Cyrus", "off"]
"Valid options for the SASL configuration parameter"
TLSOption = Literal["LibreSSL", "OpenSSL", "off"]
"Options for the TLS backend configuration parameter (AKA 'ENABLE_SSL')"
CxxVersion = Literal["r3.8.0", "r3.9.0"]
"C++ driver refs that are under CI test"

# A separator character, since we cannot use whitespace
_SEPARATOR = "\N{no-break space}\N{bullet}\N{no-break space}"


def os_split(env: EnvKey) -> tuple[str, None | str]:
    """Convert the environment key into a pretty name+version pair"""
    match env:
        # match 'alpine3.18' 'alpine53.123' etc.
        case alp if mat := re.match(r"alpine(\d+\.\d+)", alp):
            return ("Alpine", mat[1])
        case "archlinux":
            return "ArchLinux", None
        # Match 'u22', 'u20', 'u71' etc.
        case ubu if mat := re.match(r"u(\d\d)", ubu):
            return "Ubuntu", f"{mat[1]}.04"
        case _:
            raise ValueError(
                f"Failed to split OS env key {env=} into a name+version pair (unrecognized)"
            )


class EarthlyVariant(NamedTuple):
    """
    Define a "variant" that runs under a set of Earthly parameters. These are
    turned into real EVG variants later on. The Earthly arguments are passed via
    expansion parameters.
    """

    env: EnvKey
    c_compiler: CompilerName

    @property
    def display_name(self) -> str:
        """The pretty name for this variant"""
        base: str
        match os_split(self.env):
            case name, None:
                base = name
            case name, version:
                base = f"{name} {version}"
        toolchain: str
        match self.c_compiler:
            case "clang":
                toolchain = "LLVM/Clang"
            case "gcc":
                toolchain = "GCC"
        return f"{base} ({toolchain})"

    @property
    def task_selector_tag(self) -> str:
        """
        The task tag that is used to select the tasks that want to run on this
        variant.
        """
        return f"{self.env}-{self.c_compiler}"

    @property
    def expansions(self) -> Mapping[str, str]:
        """
        Expansion values that are defined for the build variant that is generated
        from this object.
        """
        return {
            _CC_PARAM_NAME: self.c_compiler,
            _ENV_PARAM_NAME: self.env,
        }

    def as_evg_variant(self) -> BuildVariant:
        return BuildVariant(
            name=f"{self.task_selector_tag}",
            tasks=[EvgTaskRef(name=f".{self.task_selector_tag}")],
            display_name=self.display_name,
            expansions=dict(self.expansions),
        )


class Configuration(NamedTuple):
    """
    Represent a complete set of argument values to give to the Earthly task
    execution. Each field name matches the ARG in the Earthfile.

    Adding/removing fields will add/remove dimensions on the task matrix.

    Some Earthly parameters are not encoded here, but are rather part of the variant (EarthlyVariant).
    """

    sasl: SASLOption
    tls: TLSOption
    test_mongocxx_ref: CxxVersion

    @property
    def suffix(self) -> str:
        return f"{_SEPARATOR}".join(f"{k}={v}" for k, v in self._asdict().items())


def task_filter(env: EarthlyVariant, conf: Configuration) -> bool:
    """
    Control which tasks are actually defined by matching on the platform and
    configuration values.
    """
    match env, conf:
        # We only need one task with "sasl=off"
        case ["u22", "gcc"], ("off", "OpenSSL", "r3.8.0"):
            return True
        # The Ubuntu 18.04 GCC has a bug that fails to build the 3.8.0 C++ driver
        case ["u18", "gcc"], [_, _, "r3.8.0"]:
            return False
        # Other sasl=off tasks we'll just ignore:
        case _, ("off", _tls, _cxx):
            return False
        # Ubuntu does not ship with a LibreSSL package:
        case e, (_sasl, "LibreSSL", _cxx) if e.display_name.startswith("Ubuntu"):
            return False
        # Anything else: Allow it to run:
        case _:
            return True


def variants_for(config: Configuration) -> Iterable[EarthlyVariant]:
    """Get all Earthly variants that are not excluded for the given build configuration"""
    all_envs = all_possible(EarthlyVariant)
    allow_env_for_config = functools.partial(task_filter, conf=config)
    return filter(allow_env_for_config, all_envs)


def earthly_exec(
    *,
    kind: Literal["test", "setup", "system"],
    target: str,
    secrets: Mapping[str, str] | None = None,
    args: Mapping[str, str] | None = None,
) -> BuiltInCommand:
    """Create a subprocess_exec command that runs Earthly with the given arguments"""
    env: dict[str, str] = {}
    if secrets:
        env["EARTHLY_SECRETS"] = ",".join(f"{k}={v}" for k, v in secrets.items())
    return subprocess_exec(
        "bash",
        args=[
            "tools/earthly.sh",
            f"+{target}",
            *(f"--{arg}={val}" for arg, val in (args or {}).items()),
        ],
        command_type=EvgCommandType(kind),
        env=env if env else None,
        working_dir="mongoc",
    )


def earthly_task(
    *,
    name: str,
    targets: Iterable[str],
    config: Configuration,
) -> Task | None:
    """
    Create an EVG task which executes earthly using the given parameters. If this
    function returns `None`, then the task configuration is excluded from executing
    and no task should be defined.
    """
    # Attach tags to the task to allow build variants to select
    # these tasks by the environment of that variant.
    env_tags = sorted(e.task_selector_tag for e in sorted(variants_for(config)))
    if not env_tags:
        # All environments have been excluded for this configuration. This means
        # the task itself should not be run:
        return
    # Generate the build-arg arguments based on the configuration options. The
    # NamedTuple field names must match with the ARG keys in the Earthfile!
    earthly_args = config._asdict()
    earthly_args |= {
        # Add arguments that come from parameter expansions defined in the build variant
        "env": f"${{{_ENV_PARAM_NAME}}}",
        "c_compiler": f"${{{_CC_PARAM_NAME}}}",
    }
    return Task(
        name=name,
        commands=[
            # First, just build the "env-warmup" which will prepare the build environment.
            # This won't generate any output, but allows EVG to track it as a separate build step
            # for timing and logging purposes. The subequent build step will cache-hit the
            # warmed-up build environments.
            earthly_exec(
                kind="setup",
                target="env-warmup",
                args=earthly_args,
            ),
            # Now execute the main tasks:
            earthly_exec(
                kind="test",
                target="run",
                # The "targets" arg is for +run to specify which targets to run
                args={"targets": " ".join(targets)} | earthly_args,
            ),
        ],
        tags=[f"earthly", "pr-merge-gate", *env_tags],
        run_on=CONTAINER_RUN_DISTROS,
    )


CONTAINER_RUN_DISTROS = [
    "ubuntu2204-small",
    "ubuntu2204-large",
    "ubuntu2004-small",
    "ubuntu2004",
    "ubuntu1804",
    "ubuntu1804-medium",
    "debian10",
    "debian11",
    "amazon2",
]


def tasks() -> Iterable[Task]:
    for conf in all_possible(Configuration):
        task = earthly_task(
            name=f"check:{conf.suffix}",
            targets=("test-example", "test-cxx-driver"),
            config=conf,
        )
        if task is not None:
            yield task


def variants() -> Iterable[BuildVariant]:
    yield from (ev.as_evg_variant() for ev in all_possible(EarthlyVariant))
