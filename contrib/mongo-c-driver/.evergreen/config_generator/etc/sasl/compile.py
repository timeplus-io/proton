from typing import ClassVar

from shrub.v3.evg_command import EvgCommand
from shrub.v3.evg_command import EvgCommandType

from config_generator.etc.utils import bash_exec

from config_generator.etc.function import Function


class CompileCommon(Function):
    ssl: ClassVar[str | None]

    @classmethod
    def compile_commands(cls, sasl=None) -> list[EvgCommand]:
        env = {}

        if cls.ssl:
            env['SSL'] = cls.ssl

        if sasl:
            env['SASL'] = sasl

        return [
            bash_exec(
                command_type=EvgCommandType.TEST,
                add_expansions_to_env=True,
                env=env,
                working_dir='mongoc',
                script='.evergreen/scripts/compile.sh',
            ),
        ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)
