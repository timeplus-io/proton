from shrub.v3.evg_command import attach_results

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class UploadTestResults(Function):
    name = 'upload-test-results'
    commands = [
        # Ensure attach_results does not fail even if no tests results exist.
        bash_exec(
            script='''\
                mkdir -p mongoc
                touch mongoc/test-results.json
            '''
        ),
        attach_results(
            file_location='mongoc/test-results.json'
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return UploadTestResults.defn()
