from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_task import EvgTaskRef

from config_generator.components.funcs.fetch_det import FetchDET
from config_generator.components.funcs.run_simple_http_server import RunSimpleHTTPServer
from config_generator.etc.utils import Task
from config_generator.etc.utils import bash_exec

def tasks():
    return [
        Task(
            name="mock-server-test",
            run_on="ubuntu2204-small",
            commands=[
                # Call fetch-det to define PYTHON3_BINARY expansion required for run-simple-http-server.
                FetchDET.call(),
                RunSimpleHTTPServer.call(),
                bash_exec(
                    command_type=EvgCommandType.TEST,
                    add_expansions_to_env=True,
                    working_dir='mongoc',
                    script='.evergreen/scripts/compile.sh',
                ),
                bash_exec(
                    command_type=EvgCommandType.TEST,
                    working_dir='mongoc',
                    script='.evergreen/scripts/run-mock-server-tests.sh',
                )
            ],
        )
    ]

def variants():
    return [
        BuildVariant(
            name="mock-server-test",
            display_name="Mock Server Test",
            tasks=[EvgTaskRef(name='mock-server-test')],
            expansions={
                'CC': 'gcc',
                'ASAN': 'on',
                'CFLAGS': '-fno-omit-frame-pointer',
                'EXTRA_CONFIGURE_FLAGS': '-DENABLE_EXTRA_ALIGNMENT=OFF',
                'SANITIZE': 'address,undefined',
            }
        ),
    ]
