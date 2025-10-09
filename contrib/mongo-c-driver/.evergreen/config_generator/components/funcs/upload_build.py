from config_generator.etc.function import Function
from shrub.v3.evg_command import archive_targz_pack
from shrub.v3.evg_command import s3_put


class UploadBuild(Function):
    name = 'upload-build'
    commands = [
        archive_targz_pack(
            target='${build_id}.tar.gz',
            source_dir='mongoc',
            include=['./**'],
        ),
        s3_put(
            aws_key='${aws_key}',
            aws_secret='${aws_secret}',
            remote_file='${project}/${build_variant}/${revision}/${task_name}/${build_id}.tar.gz',
            bucket='mciuploads',
            permissions='public-read',
            local_file='${build_id}.tar.gz',
            content_type='${content_type|application/x-gzip}',
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return UploadBuild.defn()
