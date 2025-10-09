import subprocess
import sys


def generate():
    subprocess.run(
        args=[
            sys.executable,
            '.evergreen/legacy_config_generator/generate-evergreen-config.py',
        ],
        check=True,
    )
