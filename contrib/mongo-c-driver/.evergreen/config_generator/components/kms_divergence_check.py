from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_command import s3_put
from shrub.v3.evg_task import EvgTask

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class KmsDivergenceCheck(Function):
    name = "kms-divergence-check"
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            working_dir="mongoc",
            script=".evergreen/scripts/kms-divergence-check.sh"
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return KmsDivergenceCheck.defn()


def tasks():
    return [
        EvgTask(
            name=KmsDivergenceCheck.name,
            commands=[
                KmsDivergenceCheck.call(),
            ],
        )
    ]
