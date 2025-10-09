from shrub.v3.evg_command import EvgCommandType

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class RunTests(Function):
    name = 'run-tests'
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            script='.evergreen/scripts/run-tests.sh',
            working_dir='mongoc',
            add_expansions_to_env=True,
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return RunTests.defn()
