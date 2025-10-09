#!/usr/bin/env python3

# Generates *.yml files under .evergreen/generated_configs.
#
# Install dependencies by running (preferably in a virtual environment):
#     python -m pip install -r .evergreen/config_generator/requirements.txt
#
# Invoke this using the command:
#     python3 .evergreen/config_generator/generate-config.py


import sys

from importlib import import_module


GENERATOR_NAMES = [
    "functions",
    "tasks",
    "task_groups",
    "variants",
    "legacy_config",
]


def main():
    # Requires Python 3.10 or newer.
    assert sys.version_info.major >= 3
    assert sys.version_info.minor >= 10

    for name in GENERATOR_NAMES:
        m = import_module(f"config_generator.generators.{name}")
        print(f"Running {name}.generate()...")
        m.generate()
        print(f"Running {name}.generate()... done.")


if __name__ == "__main__":
    main()
