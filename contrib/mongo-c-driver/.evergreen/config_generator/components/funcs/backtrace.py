from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class Backtrace(Function):
    name = 'backtrace'
    commands = [
        bash_exec(
            working_dir='mongoc',
            script='.evergreen/scripts/debug-core-evergreen.sh',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return Backtrace.defn()
