from shrub.v3.evg_command import archive_targz_pack
from shrub.v3.evg_command import s3_put

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class UploadMOArtifacts(Function):
    name = 'upload-mo-artifacts'
    commands = [
        bash_exec(
            working_dir='mongoc',
            script='''\
                set -o errexit
                declare dir="MO"
                if [[ -d "/cygdrive/c/data/mo" ]]; then
                    dir="/cygdrive/c/data/mo"
                fi
                if [[ -d "$dir" ]]; then
                    find "$dir" -name \\*.log | xargs tar czf mongodb-logs.tar.gz
                fi
            '''
        ),
        s3_put(
            aws_key='${aws_key}',
            aws_secret='${aws_secret}',
            bucket='mciuploads',
            content_type='${content_type|application/x-gzip}',
            display_name='mongodb-logs.tar.gz',
            local_file='mongoc/mongodb-logs.tar.gz',
            optional=True,
            permissions='public-read',
            remote_file='${project}/${build_variant}/${revision}/${version_id}/${build_id}/logs/${task_id}-${execution}-mongodb-logs.tar.gz',
        ),
        s3_put(
            aws_key='${aws_key}',
            aws_secret='${aws_secret}',
            bucket='mciuploads',
            content_type='${content_type|text/plain}',
            display_name='orchestration.log',
            local_file='mongoc/MO/server.log',
            optional=True,
            permissions='public-read',
            remote_file='${project}/${build_variant}/${revision}/${version_id}/${build_id}/logs/${task_id}-${execution}-orchestration.log',
        ),
        bash_exec(
            working_dir='mongoc',
            script='''\
                set -o errexit
                # Find all core files from mongodb in orchestration and move to mongoc
                declare dir="MO"
                if [[ -d "/cygdrive/c/data/mo" ]]; then
                    dir="/cygdrive/c/data/mo"
                fi
                declare mdmp_dir="$dir"
                if [[ -d "/cygdrive/c/mongodb" ]]; then
                    mdmp_dir="/cygdrive/c/mongodb"
                fi
                for core_file in $(find -H "$dir" "$mdmp_dir" \\( -name "*.core" -o -name "*.mdmp" \\) 2> /dev/null); do
                    declare base_name
                    base_name="$(echo "$core_file" | sed "s/.*\\///")"
                    # Move file if it does not already exist
                    if [[ ! -f "$base_name" ]]; then
                        mv "$core_file" .
                    fi
                done
            '''
        ),
        archive_targz_pack(
            target='mongo-coredumps.tgz',
            source_dir='mongoc',
            include=[
                './**.core',
                './**.mdmp',
            ],
        ),
        s3_put(
            aws_key='${aws_key}',
            aws_secret='${aws_secret}',
            bucket='mciuploads',
            content_type='${content_type|application/x-gzip}',
            display_name='Core Dumps - Execution ${execution}',
            local_file='mongo-coredumps.tgz',
            optional=True,
            permissions='public-read',
            remote_file='${project}/${build_variant}/${revision}/${version_id}/${build_id}/coredumps/${task_id}-${execution}-coredumps.log',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return UploadMOArtifacts.defn()
