# Generating the Evergreen Configuration Files

The scripts in this directory are used to generate the Evergreen configuration
files stored in `.evergreen/generated_configs/`.

The easiest way to execute these scripts is using the Poetry to install the
dependencies and then run the scripts.

**Note**: These scripts require Python 3.10 or newer.


## Setting Up

Before running, use Poetry to install a virtualenv containing the dependencies.
This can be done by using the `poetry.sh` (or `poetry.ps1`) script contained in
the `tools/` directory at the root of the `mongo-c-driver` repository:

```sh
./tools/poetry.sh install --with=dev
```

Or with PowerShell:

```pwsh
./tools/poetry.ps1 install --with=dev
```


## Running the Generator

The package defines a program `mc-evg-generate`, which can be run within the
virtualenv. This can be done via Poetry as well, following the setup:

```sh
./tools/poetry.sh run mc-evg-generate
```

This command will ready the generation files and generate a new set of Evergreen
configs in `.evergreen/config_generator/generated_configs`.

The `mc-evg-generate` command executes `main` function defined in
`config_generator/generate.py`.


## Modifying the Configs

To modify the Evergreen configuration, update the Python scripts within the
`etc/`, `config_generator/`, and `legacy_config_generator/` directories, and
execute `mc-evg-generate` again.
