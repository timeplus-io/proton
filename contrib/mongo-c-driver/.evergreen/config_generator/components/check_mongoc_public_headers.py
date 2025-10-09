from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_task import EvgTask

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class CheckMongocPublicHeaders(Function):
    name = 'check-headers'
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            working_dir='mongoc',
            script='.evergreen/scripts/check-public-decls.sh',
        ),
        bash_exec(
            command_type=EvgCommandType.TEST,
            working_dir='mongoc',
            script='.evergreen/scripts/check-preludes.py .',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return CheckMongocPublicHeaders.defn()


def tasks():
    return [
        EvgTask(
            name=CheckMongocPublicHeaders.name,
            commands=[CheckMongocPublicHeaders.call()],
        )
    ]
