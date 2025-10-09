from typing import ClassVar
from typing import Mapping
from collections import ChainMap

from shrub.v3.evg_command import EvgCommand
from shrub.v3.evg_command import FunctionCall


class Function:
    name: ClassVar[str]
    commands: ClassVar[list[EvgCommand]]

    @classmethod
    def defn(cls) -> Mapping[str, list[EvgCommand]]:
        return {cls.name: cls.commands}

    @classmethod
    def default_call(cls, **kwargs) -> FunctionCall:
        return FunctionCall(func=cls.name, **kwargs)


def merge_defns(*args):
    return ChainMap(*args)
