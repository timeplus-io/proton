from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.distros import find_large_distro
from config_generator.etc.distros import make_distro_str
from config_generator.etc.distros import to_cc
from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec
from config_generator.etc.utils import Task

SSL = 'openssl-static'
TAG = f'{SSL}-matrix'


# pylint: disable=line-too-long
# fmt: off
MATRIX = [
  ('debian92',   'gcc', None),
  ('debian10',   'gcc', None),
  ('debian11',   'gcc', None),
  ('ubuntu2004', 'gcc', None),
]
# fmt: on
# pylint: enable=line-too-long


class StaticOpenSSLCompile(Function):
    name = 'openssl-static-compile'
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            add_expansions_to_env=True,
            working_dir='mongoc',
            script='.evergreen/scripts/compile-openssl-static.sh',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return StaticOpenSSLCompile.defn()


def tasks():
    res = []

    for distro_name, compiler, arch, in MATRIX:
        tags = [TAG, distro_name, compiler]

        distro = find_large_distro(distro_name)

        compile_vars = None
        compile_vars = {'CC': to_cc(compiler)}

        if arch:
            tags.append(arch)
            compile_vars.update({'MARCH': arch})

        distro_str = make_distro_str(distro_name, compiler, arch)

        task_name = f'openssl-static-compile-{distro_str}'

        res.append(
            Task(
                name=task_name,
                run_on=distro.name,
                tags=tags,
                commands=[StaticOpenSSLCompile.call(vars=compile_vars)],
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
