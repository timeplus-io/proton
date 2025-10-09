from config_generator.etc.compile import generate_compile_tasks

from config_generator.etc.sanitizers.test import generate_test_tasks

from config_generator.components.sasl.openssl import SaslCyrusOpenSSLCompile

from config_generator.components.sanitizers.asan import TAG


# pylint: disable=line-too-long
# fmt: off
COMPILE_MATRIX = [
    ('ubuntu1604', 'clang', None, ['cyrus']),
    ('ubuntu1804', 'clang', None, ['cyrus']),
    ('ubuntu2004', 'clang', None, ['cyrus']),
]

TEST_MATRIX = [
    ('ubuntu1604', 'clang', None, 'cyrus', ['auth'], ['server', 'replica', 'sharded'], ['3.6',                                  ]),
    ('ubuntu1804', 'clang', None, 'cyrus', ['auth'], ['server', 'replica', 'sharded'], [       '4.0', '4.2', '4.4', '5.0', '6.0']),

    # Test 7.0+ with Ubuntu 20.04+ since MongoDB 7.0 no longer ships binaries for Ubuntu 18.04.
    ('ubuntu2004', 'clang', None, 'cyrus', ['auth'], ['server', 'replica', 'sharded'], ['7.0', 'latest']),
]
# fmt: on
# pylint: enable=line-too-long


def tasks():
    res = []

    SSL = 'openssl'

    SASL_TO_FUNC = {
        'cyrus': SaslCyrusOpenSSLCompile,
    }

    res += generate_compile_tasks(
        SSL, TAG, SASL_TO_FUNC, COMPILE_MATRIX, ['asan']
    )

    res += generate_test_tasks(SSL, TAG, TEST_MATRIX, ['asan'])

    return res
