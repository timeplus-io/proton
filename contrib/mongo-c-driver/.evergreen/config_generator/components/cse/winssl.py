from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.compile import generate_compile_tasks

from config_generator.etc.cse.compile import CompileCommon
from config_generator.etc.cse.test import generate_test_tasks


SSL = 'winssl'
TAG = f'cse-matrix-{SSL}'


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('windows-64-vs2015', 'vs2015x64', None, ['cyrus']),
    ('windows-vsCurrent', 'vs2017x64', None, ['cyrus']),
]

# TODO (CDRIVER-3789): test cse with the 'sharded' topology.
TEST_MATRIX = [
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server'], ['4.2', '4.4', '5.0', '6.0'                ]),

    # Test 7.0+ with a replica set since Queryable Encryption does not support the 'server' topology. Queryable Encryption tests require 7.0+.
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server', 'replica' ], [               '7.0', 'latest']),
]
# fmt: on
# pylint: enable=line-too-long


class WinSSLCompileCommon(CompileCommon):
    ssl = 'WINDOWS'


class SaslCyrusWinSSLCompile(WinSSLCompileCommon):
    name = 'cse-sasl-cyrus-winssl-compile'
    commands = WinSSLCompileCommon.compile_commands(sasl='CYRUS')


def functions():
    return SaslCyrusWinSSLCompile.defn()


def tasks():
    res = []

    SASL_TO_FUNC = {
        'cyrus': SaslCyrusWinSSLCompile,
    }

    MORE_TAGS = ['cse']

    res += generate_compile_tasks(
        SSL, TAG, SASL_TO_FUNC, COMPILE_MATRIX, MORE_TAGS
    )

    res += generate_test_tasks(SSL, TAG, TEST_MATRIX)

    return res


def variants():
    expansions = {
        'CLIENT_SIDE_ENCRYPTION': 'on',
        'DEBUG': 'ON',
    }

    return [
        BuildVariant(
            name=TAG,
            display_name=TAG,
            tasks=[EvgTaskRef(name=f'.{TAG}')],
            expansions=expansions,
        ),
    ]
