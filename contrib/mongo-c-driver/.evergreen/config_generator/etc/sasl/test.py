from itertools import product

from shrub.v3.evg_command import expansions_update
from shrub.v3.evg_command import KeyValueParam
from shrub.v3.evg_task import EvgTaskDependency

from config_generator.etc.distros import find_small_distro
from config_generator.etc.distros import make_distro_str
from config_generator.etc.distros import to_cc
from config_generator.etc.utils import Task

from config_generator.components.funcs.bootstrap_mongo_orchestration import BootstrapMongoOrchestration
from config_generator.components.funcs.fetch_build import FetchBuild
from config_generator.components.funcs.fetch_det import FetchDET
from config_generator.components.funcs.run_simple_http_server import RunSimpleHTTPServer
from config_generator.components.funcs.run_tests import RunTests


def generate_test_tasks(SSL, TAG, MATRIX):
    res = []

    TOPOLOGY_TO_STR = {
        'server': 'server',
        'replica': 'replica_set',
        'sharded': 'sharded_cluster',
    }

    for distro_name, compiler, arch, sasl, auths, topologies, server_vers in MATRIX:
        tags = [TAG, 'test', distro_name, compiler]
        test_distro = find_small_distro(distro_name)

        compile_vars = []
        compile_vars.append(KeyValueParam(key='CC', value=to_cc(compiler)))

        if arch:
            tags.append(arch)
            compile_vars.append(KeyValueParam(key='MARCH', value=arch))

        distro_str = make_distro_str(distro_name, compiler, arch)
        compile_task_name = f'sasl-{sasl}-{SSL}-{distro_str}-compile'

        for auth, topology, server_ver in product(auths, topologies, server_vers):
            test_tags = tags + [f'sasl-{sasl}', auth, topology, server_ver]
            # Do not add `nossl` tag to prevent being selected by legacy config variants.
            # Remove the `if` when CDRIVER-4571 is resolved.
            if SSL != 'nossl':
                test_tags += [SSL]

            test_task_name = f'sasl-{sasl}-{SSL}-{distro_str}-test-{server_ver}-{topology}-{auth}'

            test_commands = []
            test_commands.append(FetchBuild.call(build_name=compile_task_name))

            updates = compile_vars + [
                KeyValueParam(key='AUTH', value=auth),
                KeyValueParam(key='MONGODB_VERSION', value=server_ver),
                KeyValueParam(key='TOPOLOGY', value=TOPOLOGY_TO_STR[topology]),
                KeyValueParam(key='SSL', value=SSL),
            ]

            test_commands.append(expansions_update(updates=updates))
            test_commands.append(FetchDET.call())
            test_commands.append(BootstrapMongoOrchestration.call())
            test_commands.append(RunSimpleHTTPServer.call())
            test_commands.append(RunTests.call())

            res.append(
                Task(
                    name=test_task_name,
                    run_on=test_distro.name,
                    tags=test_tags,
                    depends_on=[EvgTaskDependency(name=compile_task_name)],
                    commands=test_commands,
                )
            )

    return res
