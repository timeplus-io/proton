from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.function import merge_defns
from config_generator.etc.compile import generate_compile_tasks

from config_generator.etc.sasl.compile import CompileCommon
from config_generator.etc.sasl.test import generate_test_tasks


SSL = 'darwinssl'
TAG = f'sasl-matrix-{SSL}'


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('macos-1100',       'clang', None, ['cyrus']),
    ('macos-1100-arm64', 'clang', None, ['cyrus']),
]

TEST_MATRIX = [
    ('macos-1100', 'clang', None, 'cyrus', ['auth'], ['server'], ['3.6', '4.0', '4.2', '4.4', '5.0', '6.0', '7.0', 'latest']),
]
# fmt: on
# pylint: enable=line-too-long


class DarwinSSLCompileCommon(CompileCommon):
    ssl = 'DARWIN'


class SaslCyrusDarwinSSLCompile(DarwinSSLCompileCommon):
    name = 'sasl-cyrus-darwinssl-compile'
    commands = DarwinSSLCompileCommon.compile_commands(sasl='CYRUS')


def functions():
    return merge_defns(
        SaslCyrusDarwinSSLCompile.defn(),
    )


def tasks():
    res = []

    SASL_TO_FUNC = {
        'cyrus': SaslCyrusDarwinSSLCompile,
    }

    res += generate_compile_tasks(SSL, TAG, SASL_TO_FUNC, COMPILE_MATRIX)
    res += generate_test_tasks(SSL, TAG, TEST_MATRIX)

    return res


def variants():
    expansions = {
        'DEBUG': 'ON'
    }

    return [
        BuildVariant(
            name=TAG,
            display_name=TAG,
            tasks=[EvgTaskRef(name=f'.{TAG}')],
            expansions=expansions,
        ),
    ]
