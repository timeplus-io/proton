from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.distros import find_large_distro
from config_generator.etc.distros import make_distro_str
from config_generator.etc.distros import to_cc
from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec
from config_generator.etc.utils import Task


TAG = 'std-matrix'


# pylint: disable=line-too-long
# fmt: off
MATRIX = [
    ('debian92',          'clang',     None,   [11,   ]),
    ('ubuntu1604',        'clang',     'i686', [11,   ]),
    ('ubuntu1604',        'clang',     None,   [11,   ]),
    ('ubuntu1804',        'clang',     'i686', [11,   ]),
    ('ubuntu1804',        'gcc',       None,   [11,   ]),
    ('debian10',          'clang',     None,   [11,   ]),
    ('debian10',          'gcc',       None,   [11, 17]),
    ('debian11',          'clang',     None,   [11,   ]),
    ('debian11',          'gcc',       None,   [11, 17]),
    ('ubuntu2004',        'clang',     None,   [11,   ]),
    ('ubuntu2004',        'gcc',       None,   [11,   ]),
    ('windows-vsCurrent', 'vs2017x64', None,   [11, 17]),
]
# fmt: on
# pylint: enable=line-too-long


class StdCompile(Function):
    name = 'std-compile'
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            add_expansions_to_env=True,
            working_dir='mongoc',
            script='.evergreen/scripts/compile-std.sh',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return StdCompile.defn()


def tasks():
    res = []

    for distro_name, compiler, arch, stds in MATRIX:
        tags = [TAG, distro_name, compiler, 'compile']

        distro = find_large_distro(distro_name)

        compile_vars = None
        compile_vars = {'CC': to_cc(compiler)}

        if arch:
            tags.append(arch)
            compile_vars.update({'MARCH': arch})

        distro_str = make_distro_str(distro_name, compiler, arch)

        for std in stds:
            with_std = {'C_STD_VERSION': std}

            task_name = f'std-c{std}-{distro_str}-compile'

            res.append(
                Task(
                    name=task_name,
                    run_on=distro.name,
                    tags=tags + [f'std-c{std}'],
                    commands=[StdCompile.call(vars=compile_vars | with_std)],
                )
            )

    return res


def variants():
    return [
        BuildVariant(
            name=TAG,
            display_name=TAG,
            tasks=[EvgTaskRef(name=f'.{TAG}')],
        ),
    ]
