<#
.SYNOPSIS
    Execute a pinned and cached installation of Poetry
.DESCRIPTION
    This command will execute a Poetry command subprocess. It first looks for
    an installed version, installs it if it is absent, and then uses the
    installed Poetry to execute the requested command.

    For help from Poetry, Use the 'poetry.ps1 help' command as seen in example 1.
.EXAMPLE
    > poetry.ps1 help

    Print help messages from Poetry itself
.EXAMPLE
    > poetry.ps1 install --with=docs --with=env

    Installs the project dependencies in the active virtualenv. This operation
    is idempotent
.EXAMPLE
    > poetry.ps1 shell

    Spawn a new interactive PowerShell session with the project's virtualenv
    active. With this, the project's dependencies will be available on
    $env:PATH.
.EXAMPLE
    > poetry.ps1 run sphinx-build -bhtml docs/ out/docs/html

    Execute the sphinx-build executable that is installed within the
    Poetry-managed virtual environment.
.LINK
    https://python-poetry.org

        The Poetry website.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    # The version of Poetry that we want to install. By default, installs 1.4.2
    [string]
    $PoetryVersion = "1.4.2",
    # Force the directory in which Poetry will be installed
    [string]
    $PoetryHome,
    # The Poetry subcommand to execute. (If any Poetry arguments conflict with
    # PowerShell argument parsing, enquote them to ensure they are passed as
    # positional string arguments.)
    [Parameter(Position = 2, ValueFromRemainingArguments)]
    [string[]]
    $Command
)

# Halt on subcommand errors
$ErrorActionPreference = 'Stop'

# We assume Windows and pwsh 7+
$is_pwsh_win = $PSVersionTable.Platform -eq "Win32NT" -or $PSVersionTable.PSEdition -eq "Desktop"
if (-not $is_pwsh_win) {
    throw "This script is meant for PowerShell 7+ on Windows. (Use 'pwsh' on Windows, and use poetry.sh for Unix-like systems.)"
}

# The directory that contains this file:
$this_dir = $PSScriptRoot

# Load util vars and functions:
. "$this_dir/init.ps1"

# The directory in which we are installing Poetry:
if ([String]::IsNullOrEmpty($PoetryHome)) {
    $PoetryHome = "$BUILD_CACHE_DIR/poetry-$PoetryVersion"
}
$stamp_file = Join-Path $PoetryHome ".installed.done"

# Poetry respects the POETRY_HOME environment variable.
$env:POETRY_HOME = $PoetryHome

$py = Find-Python

# Create the Poetry installation if it is not already present
if (!(Test-Path $stamp_file)) {
    Write-Debug "Installing Poetry $PoetryVersion into [$PoetryHome]"
    # Lock to prevent concurrent installations
    $mtx = New-Object System.Threading.Mutex($false, "Global\MongoCPoetryInstall")
    [void]$mtx.WaitOne()
    # (No need to double-check, since a second install is a no-op)
    try {
        # Do the actual install:
        & $py -u "$this_dir/install-poetry.py" --yes --version "$PoetryVersion"
        if ($LASTEXITCODE -ne 0) {
            throw "Poetry installation failed [$LASTEXITCODE]"
        }
        # Touch the stamp file to tell future runs that the install is okay:
        Set-Content $stamp_file ""
    }
    finally {
        [void]$mtx.ReleaseMutex()
    }
}

# Execute the Poetry command
$poetry_exe = Join-Path $PoetryHome "bin/poetry.exe"
& $poetry_exe env use --quiet $py
& $poetry_exe @Command
exit $LASTEXITCODE
