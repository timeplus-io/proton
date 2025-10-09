from shrub.v3.evg_command import archive_targz_extract
from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_command import s3_get

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class FetchBuild(Function):
    name = 'fetch-build'
    command_type = EvgCommandType.SETUP
    commands = [
        bash_exec(command_type=command_type, script='rm -rf mongoc'),
        s3_get(
            command_type=command_type,
            aws_key='${aws_key}',
            aws_secret='${aws_secret}',
            bucket='mciuploads',
            local_file='build.tar.gz',
            remote_file='${project}/${build_variant}/${revision}/${BUILD_NAME}/${build_id}.tar.gz',
        ),
        archive_targz_extract(path='build.tar.gz', destination='mongoc'),
        # Scripts may not be executable on Windows.
        bash_exec(
            command_type=command_type,
            working_dir='mongoc',
            script='''\
                for file in $(find .evergreen/scripts -type f); do
                    chmod +rx "$file" || exit
                done
            '''
        ),
    ]

    @classmethod
    def call(cls, build_name, vars=None, **kwargs):
        if vars is None:
            vars = {}
        vars.update({'BUILD_NAME': build_name})
        return cls.default_call(vars=vars, **kwargs)


def functions():
    return FetchBuild.defn()
