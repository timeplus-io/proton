from shrub.v3.evg_command import EvgCommandType

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class RunSimpleHTTPServer(Function):
    name = 'run-simple-http-server'
    command_type = EvgCommandType.SETUP
    commands = [
        bash_exec(
            command_type=command_type,
            background=True,
            working_dir='mongoc',
            script='''\
                set -o errexit
                echo "Starting simple HTTP server..."
                command -V "${PYTHON3_BINARY}" >/dev/null
                "${PYTHON3_BINARY}" .evergreen/scripts/simple_http_server.py
                echo "Starting simple HTTP server... done."
            ''',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return RunSimpleHTTPServer.defn()
