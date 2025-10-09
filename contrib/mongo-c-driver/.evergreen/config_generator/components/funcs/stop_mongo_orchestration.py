from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class StopMongoOrchestration(Function):
    name = 'stop-mongo-orchestration'
    commands = [
        bash_exec(
            script='''\
                if [[ -d MO ]]; then
                    cd MO && mongo-orchestration stop
                fi
            '''
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return StopMongoOrchestration.defn()
