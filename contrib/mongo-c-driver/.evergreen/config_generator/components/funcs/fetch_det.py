from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_command import expansions_update

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class FetchDET(Function):
    name = 'fetch-det'
    commands = [
        bash_exec(
            command_type=EvgCommandType.SETUP,
            script='''\
                if [[ ! -d drivers-evergreen-tools ]]; then
                    git clone --depth=1 https://github.com/mongodb-labs/drivers-evergreen-tools.git
                fi
            ''',
        ),

        # Make shell scripts executable.
        bash_exec(
            command_type=EvgCommandType.SETUP,
            working_dir="drivers-evergreen-tools",
            script='find .evergreen -type f -name "*.sh" -exec chmod +rx "{}" \;',
        ),

        # python is used frequently enough by many tasks that it is worth
        # running find_python3 once here and reusing the result.
        bash_exec(
            command_type=EvgCommandType.SETUP,
            script='''\
                set -o errexit
                . drivers-evergreen-tools/.evergreen/find-python3.sh
                echo "PYTHON3_BINARY: $(find_python3)" >|python3_binary.yml
            ''',
        ),
        expansions_update(
            command_type=EvgCommandType.SETUP,
            file='python3_binary.yml',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return FetchDET.defn()
