from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_command import expansions_update
from shrub.v3.evg_command import git_get_project

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class FetchSource(Function):
    name = 'fetch-source'
    command_type = EvgCommandType.SETUP
    commands = [
        git_get_project(command_type=command_type, directory='mongoc'),
        bash_exec(
            command_type=command_type,
            working_dir='mongoc',
            script='''\
                set -o errexit
                set -o pipefail
                if [ -n "${github_pr_number}" -o "${is_patch}" = "true" ]; then
                    # This is a GitHub PR or patch build, probably branched from master
                    if command -v python3 &>/dev/null; then
                        # Prefer python3 if it is available
                        echo $(python3 ./build/calc_release_version.py --next-minor) > VERSION_CURRENT
                    else
                        echo $(python ./build/calc_release_version.py --next-minor) > VERSION_CURRENT
                    fi
                    VERSION=$VERSION_CURRENT-${version_id}
                else
                    VERSION=latest
                fi
                echo "CURRENT_VERSION: $VERSION" > expansion.yml
            '''
        ),
        expansions_update(command_type=command_type,
                          file='mongoc/expansion.yml'),
        # Scripts may not be executable on Windows.
        bash_exec(
            command_type=EvgCommandType.SETUP,
            working_dir='mongoc',
            script='''\
                for file in $(find .evergreen/scripts -type f); do
                    chmod +rx "$file" || exit
                done
            '''
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return FetchSource.defn()
