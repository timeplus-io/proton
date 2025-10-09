from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.function import merge_defns
from config_generator.etc.compile import generate_compile_tasks

from config_generator.etc.sasl.compile import CompileCommon
from config_generator.etc.sasl.test import generate_test_tasks


SSL = 'winssl'
TAG = f'sasl-matrix-{SSL}'


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('windows-64-vs2013', 'vs2013x64', None, [       'cyrus',       ]),
    ('windows-64-vs2013', 'vs2013x86', None, ['off',                ]),
    ('windows-64-vs2015', 'vs2015x64', None, [       'cyrus',       ]),
    ('windows-64-vs2015', 'vs2015x86', None, ['off',                ]),
    ('windows-vsCurrent', 'mingw',     None, [                'sspi']),
    ('windows-vsCurrent', 'vs2017x64', None, ['off', 'cyrus', 'sspi']),
    ('windows-vsCurrent', 'vs2017x86', None, ['off',          'sspi']),
]

TEST_MATRIX = [
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server'], ['3.6', '4.0', '4.2', '4.4', '5.0', '6.0', '7.0', 'latest']),

    ('windows-vsCurrent', 'mingw',     None, 'sspi',  ['auth'], ['server'], ['latest']),
    ('windows-vsCurrent', 'vs2017x64', None, 'sspi',  ['auth'], ['server'], ['latest']),
    ('windows-vsCurrent', 'vs2017x86', None, 'sspi',  ['auth'], ['server'], ['latest']),
]
# fmt: on
# pylint: enable=line-too-long


class WinSSLCompileCommon(CompileCommon):
    ssl = 'WINDOWS'


class SaslOffWinSSLCompile(WinSSLCompileCommon):
    name = 'sasl-off-winssl-compile'
    commands = WinSSLCompileCommon.compile_commands(sasl='OFF')


class SaslCyrusWinSSLCompile(WinSSLCompileCommon):
    name = 'sasl-cyrus-winssl-compile'
    commands = WinSSLCompileCommon.compile_commands(sasl='CYRUS')


class SaslSspiWinSSLCompile(WinSSLCompileCommon):
    name = 'sasl-sspi-winssl-compile'
    commands = WinSSLCompileCommon.compile_commands(sasl='SSPI')


def functions():
    return merge_defns(
        SaslOffWinSSLCompile.defn(),
        SaslCyrusWinSSLCompile.defn(),
        SaslSspiWinSSLCompile.defn(),
    )


def tasks():
    res = []

    SASL_TO_FUNC = {
        'off': SaslOffWinSSLCompile,
        'cyrus': SaslCyrusWinSSLCompile,
        'sspi': SaslSspiWinSSLCompile,
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
