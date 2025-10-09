from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_task import EvgTask
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.distros import find_large_distro
from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


TAG = "clang-format"

DISTROS = [
    find_large_distro("ubuntu2204").name,
    find_large_distro("ubuntu2004").name,
]


class ClangFormat(Function):
    name = TAG
    commands = [
        bash_exec(
            command_type=EvgCommandType.SETUP,
            working_dir="mongoc",
            script="./tools/poetry.sh install --with=dev"
        ),
        bash_exec(
            command_type=EvgCommandType.TEST,
            working_dir="mongoc",
            env={
                "DRYRUN": "1",
            },
            script="./tools/poetry.sh run .evergreen/scripts/clang-format-all.sh",
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return ClangFormat.defn()


def tasks():
    yield EvgTask(
        name=TAG,
        tags=[TAG],
        commands=[ClangFormat.call()],
    )


def variants():
    return [
        BuildVariant(
            name=TAG,
            display_name=TAG,
            run_on=DISTROS,
            tasks=[EvgTaskRef(name=f'.{TAG}')],
        ),
    ]
