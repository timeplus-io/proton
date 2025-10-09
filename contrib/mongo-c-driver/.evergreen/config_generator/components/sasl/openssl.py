from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from config_generator.etc.function import merge_defns
from config_generator.etc.compile import generate_compile_tasks

from config_generator.etc.sasl.compile import CompileCommon
from config_generator.etc.sasl.test import generate_test_tasks


SSL = 'openssl'
TAG = f'sasl-matrix-{SSL}'


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('debian10',          'gcc',        None, ['cyrus']),
    ('debian11',          'gcc',        None, ['cyrus']),
    ('debian92',          'clang',      None, ['cyrus']),
    ('debian92',          'gcc',        None, ['cyrus']),
    ('rhel70',            'gcc',        None, ['cyrus']),
    ('rhel80',            'gcc',        None, ['cyrus']),
    ('rhel81-power8',     'gcc',        None, ['cyrus']),
    ('rhel83-zseries',    'gcc',        None, ['cyrus']),
    ('ubuntu1604-arm64',  'gcc',        None, ['cyrus']),
    ('ubuntu1604',        'clang',      None, ['cyrus']),
    ('ubuntu1804-arm64',  'gcc',        None, ['cyrus']),
    ('ubuntu1804',        'gcc',        None, ['cyrus']),
    ('ubuntu2004-arm64',  'gcc',        None, ['cyrus']),
    ('ubuntu2004',        'gcc',        None, ['cyrus']),
    ('windows-vsCurrent', 'vs2017x64',  None, ['cyrus']),
]

TEST_MATRIX = [
    ('rhel81-power8',     'gcc',       None, 'cyrus', ['auth'], ['server',          ], [       '4.2', '4.4', '5.0', '6.0', '7.0', 'latest']),
    ('rhel83-zseries',    'gcc',       None, 'cyrus', ['auth'], ['server',          ], [                     '5.0', '6.0', '7.0', 'latest']),
    ('ubuntu1804-arm64',  'gcc',       None, 'cyrus', ['auth'], ['server',          ], [       '4.2', '4.4', '5.0', '6.0',                ]),
    ('ubuntu1804',        'gcc',       None, 'cyrus', ['auth'], ['server', 'replica'], ['4.0', '4.2', '4.4', '5.0', '6.0',                ]),

    # Test 7.0+ with Ubuntu 20.04+ since MongoDB 7.0 no longer ships binaries for Ubuntu 18.04.
    ('ubuntu2004-arm64',  'gcc',       None, 'cyrus', ['auth'], ['server'], ['7.0', 'latest']),
    ('ubuntu2004',        'gcc',       None, 'cyrus', ['auth'], ['server'], ['7.0', 'latest']),
    ('windows-vsCurrent', 'vs2017x64', None, 'cyrus', ['auth'], ['server'], [       'latest']),

    # Test ARM64 + 4.0 on Ubuntu 16.04, as MongoDB server does not produce
    # downloads for Ubuntu 18.04 arm64.
    # See: https://www.mongodb.com/docs/manual/administration/production-notes/
    ('ubuntu1604-arm64', 'gcc', None, 'cyrus', ['auth'], ['server'], ['4.0']),
]
# fmt: on
# pylint: enable=line-too-long


class OpenSSLCompileCommon(CompileCommon):
    ssl = 'OPENSSL'


class SaslCyrusOpenSSLCompile(OpenSSLCompileCommon):
    name = 'sasl-cyrus-openssl-compile'
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
