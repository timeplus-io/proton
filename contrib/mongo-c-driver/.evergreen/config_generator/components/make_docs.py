from shrub.v3.evg_command import EvgCommandType
from shrub.v3.evg_command import s3_put
from shrub.v3.evg_task import EvgTask

from config_generator.etc.function import Function
from config_generator.etc.function import merge_defns
from config_generator.etc.utils import bash_exec


class MakeDocs(Function):
    name = "make-docs"
    commands = [
        bash_exec(
            command_type=EvgCommandType.TEST,
            working_dir="mongoc",
            include_expansions_in_env=["distro_id"],
            script="""\
                set -o errexit
                bash tools/poetry.sh install --with=docs
                # See SphinxBuild.cmake for EVG_DOCS_BUILD reasoning
                bash tools/poetry.sh run env EVG_DOCS_BUILD=1 bash .evergreen/scripts/build-docs.sh
                """,
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


class UploadDocs(Function):
    name = "upload-docs"
    commands = [
        bash_exec(
            working_dir="mongoc/_build/for-docs/src/libbson",
            env={
                "AWS_ACCESS_KEY_ID": "${aws_key}",
                "AWS_SECRET_ACCESS_KEY": "${aws_secret}",
            },
            script="aws s3 cp doc/html s3://mciuploads/${project}/docs/libbson/${CURRENT_VERSION} --quiet --recursive --acl public-read --region us-east-1",
        ),
        s3_put(
            aws_key="${aws_key}",
            aws_secret="${aws_secret}",
            bucket="mciuploads",
            content_type="text/html",
            display_name="libbson docs",
            local_file="mongoc/_build/for-docs/src/libbson/doc/html/index.html",
            permissions="public-read",
            remote_file="${project}/docs/libbson/${CURRENT_VERSION}/index.html",
        ),
        bash_exec(
            working_dir="mongoc/_build/for-docs/src/libmongoc",
            env={
                "AWS_ACCESS_KEY_ID": "${aws_key}",
                "AWS_SECRET_ACCESS_KEY": "${aws_secret}",
            },
            script="aws s3 cp doc/html s3://mciuploads/${project}/docs/libmongoc/${CURRENT_VERSION} --quiet --recursive --acl public-read --region us-east-1",
        ),
        s3_put(
            aws_key="${aws_key}",
            aws_secret="${aws_secret}",
            bucket="mciuploads",
            content_type="text/html",
            display_name="libmongoc docs",
            local_file="mongoc/_build/for-docs/src/libmongoc/doc/html/index.html",
            permissions="public-read",
            remote_file="${project}/docs/libmongoc/${CURRENT_VERSION}/index.html",
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


class UploadManPages(Function):
    name = "upload-man-pages"
    commands = [
        bash_exec(
            working_dir="mongoc",
            silent=True,
            script="""\
                set -o errexit
                # Get "aha", the ANSI HTML Adapter.
                git clone --depth 1 https://github.com/theZiz/aha.git aha-repo
                pushd aha-repo
                make
                popd # aha-repo
                mv aha-repo/aha .
                .evergreen/scripts/man-pages-to-html.sh libbson _build/for-docs/src/libbson/doc/man > bson-man-pages.html
                .evergreen/scripts/man-pages-to-html.sh libmongoc _build/for-docs/src/libmongoc/doc/man > mongoc-man-pages.html
                """,
        ),
        s3_put(
            aws_key="${aws_key}",
            aws_secret="${aws_secret}",
            bucket="mciuploads",
            content_type="text/html",
            display_name="libbson man pages",
            local_file="mongoc/bson-man-pages.html",
            permissions="public-read",
            remote_file="${project}/man-pages/libbson/${CURRENT_VERSION}/index.html",
        ),
        s3_put(
            aws_key="${aws_key}",
            aws_secret="${aws_secret}",
            bucket="mciuploads",
            content_type="text/html",
            display_name="libmongoc man pages",
            local_file="mongoc/mongoc-man-pages.html",
            permissions="public-read",
            remote_file="${project}/man-pages/libmongoc/${CURRENT_VERSION}/index.html",
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return merge_defns(
        MakeDocs.defn(),
        UploadDocs.defn(),
        UploadManPages.defn(),
    )


def tasks():
    return [
        EvgTask(
            name="make-docs",
            commands=[
                MakeDocs.call(),
                UploadDocs.call(),
                UploadManPages.call(),
            ],
        )
    ]
