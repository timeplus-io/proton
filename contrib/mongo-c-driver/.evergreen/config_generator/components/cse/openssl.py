from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.compile import generate_compile_tasks
from config_generator.etc.function import merge_defns

from config_generator.etc.cse.compile import CompileCommon
from config_generator.etc.cse.test import generate_test_tasks


SSL = 'openssl'
TAG = f'cse-matrix-{SSL}'


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('debian10',          'gcc',       None, ['cyrus']),
    ('debian11',          'gcc',       None, ['cyrus']),
    ('debian92',          'clang',     None, ['cyrus']),
    ('debian92',          'gcc',       None, ['cyrus']),
    ('rhel80',            'gcc',       None, ['cyrus']),
    ('rhel83-zseries',    'gcc',       None, ['cyrus']),
    ('ubuntu1604',        'clang',     None, ['cyrus']),
    ('ubuntu1804-arm64',  'gcc',       None, ['cyrus']),
    ('ubuntu1804',        'gcc',       None, ['cyrus']),
    ('ubuntu2004',        'gcc',       None, ['cyrus']),
    ('ubuntu2004-arm64',  'gcc',       None, ['cyrus']),
    ('windows-vsCurrent', 'vs2017x64', None, ['cyrus']),
]

# TODO (CDRIVER-3789): test cse with the 'sharded' topology.
TEST_MATRIX = [
    # 4.2 and 4.4 not available on rhel83-zseries.
    ('rhel83-zseries', 'gcc', None, 'cyrus', ['auth'], ['server'], ['5.0']),

    ('ubuntu1804-arm64',  'gcc',       None, 'cyrus', ['auth'], ['server'], ['4.2', '4.4', '5.0', '6.0' ]),
    ('ubuntu1804',        'gcc',       None, 'cyrus', ['auth'], ['server'], ['4.2', '4.4', '5.0', '6.0' ]),
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server'], ['4.2', '4.4', '5.0', '6.0' ]),

    # Test 7.0+ with a replica set since Queryable Encryption does not support the 'server' topology. Queryable Encryption tests require 7.0+.
    # Test 7.0+ with Ubuntu 20.04+ since MongoDB 7.0 no longer ships binaries for Ubuntu 18.04.
    ('ubuntu2004',        'gcc',       None, 'cyrus', ['auth'], ['server', 'replica'], [ '7.0', 'latest']),
    ('rhel83-zseries',    'gcc',       None, 'cyrus', ['auth'], ['server', 'replica'], [ '7.0', 'latest']),
    ('ubuntu2004-arm64',  'gcc',       None, 'cyrus', ['auth'], ['server', 'replica'], [ '7.0', 'latest']),
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server', 'replica'], [ '7.0', 'latest']),
]
# fmt: on
# pylint: enable=line-too-long


class OpenSSLCompileCommon(CompileCommon):
    ssl = 'OPENSSL'


class SaslCyrusOpenSSLCompile(OpenSSLCompileCommon):
    name = 'cse-sasl-cyrus-openssl-compile'
    commands = OpenSSLCompileCommon.compile_commands(sasl='CYRUS')


def functions():
    return merge_defns(
        SaslCyrusOpenSSLCompile.defn(),
    )


def tasks():
    res = []

    SASL_TO_FUNC = {
        'cyrus': SaslCyrusOpenSSLCompile,
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
