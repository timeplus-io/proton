from shrub.v3.evg_command import EvgCommandType

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class RunMockKMSServers(Function):
    name = 'run-mock-kms-servers'
    command_type = EvgCommandType.SETUP
    commands = [
        # This command ensures future invocations of activate-kmstlsvenv.sh conducted in
        # parallel do not race to setup a venv environment; it has already been prepared.
        # This primarily addresses the situation where the "run tests" and "run-mock-kms-servers"
        # functions invoke 'activate-kmstlsvenv.sh' simultaneously.
        # TODO: remove this function along with the "run-mock-kms-servers" function.
        bash_exec(
            command_type=command_type,
            working_dir='drivers-evergreen-tools/.evergreen/csfle',
            script='''\
                set -o errexit
                echo "Preparing KMS TLS venv environment..."
                # TODO: remove this function along with the "run kms servers" function.
                if [[ "$OSTYPE" =~ cygwin && ! -d kmstlsvenv ]]; then
                    # Avoid using Python 3.10 on Windows due to incompatible cipher suites.
                    # See CDRIVER-4530.
                    . ../venv-utils.sh
                    venvcreate "C:/python/Python39/python.exe" kmstlsvenv || # windows-2017
                    venvcreate "C:/python/Python38/python.exe" kmstlsvenv    # windows-2015
                    python -m pip install --upgrade boto3~=1.19 pykmip~=0.10.0 "sqlalchemy<2.0.0"
                    deactivate
                else
                    . ./activate-kmstlsvenv.sh
                    deactivate
                fi
                echo "Preparing KMS TLS venv environment... done."
            ''',
        ),
        bash_exec(
            command_type=command_type,
            background=True,
            working_dir='drivers-evergreen-tools/.evergreen/csfle',
            script='''\
                set -o errexit
                echo "Starting mock KMS TLS servers..."
                . ./activate-kmstlsvenv.sh
                python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --port 8999 &
                python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/expired.pem --port 9000 &
                python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/wrong-host.pem --port 9001 &
                python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --require_client_cert --port 9002 &
                python -u kms_kmip_server.py &
                deactivate
                echo "Starting mock KMS TLS servers... done."
            ''',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return RunMockKMSServers.defn()
