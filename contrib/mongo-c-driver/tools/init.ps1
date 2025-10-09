$ErrorActionPreference = "Stop"

$is_windows = $PSVersionTable.Platform -eq "Win32NT" -or $PSVersionTable.PSEdition -eq "Desktop"

if (-not $is_windows) {
    throw "This script is meant for Windows only."
}

$USER_CACHES_DIR = $env:USER_CACHES_DIR
if ([String]::IsNullOrEmpty($USER_CACHES_DIR)) {
    $USER_CACHES_DIR = $env:LOCALAPPDATA
}

$BUILD_CACHE_BUST = $env:BUILD_CACHE_BUST
if ([String]::IsNullOrEmpty($BUILD_CACHE_BUST)) {
    $BUILD_CACHE_BUST = "1"
}
$BUILD_CACHE_DIR = Join-Path $USER_CACHES_DIR "mongoc.$BUILD_CACHE_BUST"
Write-Debug "Calculated mongoc build cache directory to be [$BUILD_CACHE_DIR]"


function Test-Command {
    param (
        [string[]]$Name,
        [System.Management.Automation.CommandTypes]$CommandType = 'All'
    )
    $found = @(Get-Command -Name:$Name -CommandType:$CommandType -ErrorAction Ignore -TotalCount 1)
    if ($found.Length -ne 0) {
        return $true
    }
    return $false
}

function Find-Python {
    if (Test-Command "py" -CommandType Application -and (& py -c "(x:=0)")) {
        $found = "py"
    }
    else {
        foreach ($n in 20..8) {
            $cand = "python3.$n"
            Write-Debug "Try Python: [$cand]"
            if (!(Test-Command $cand -CommandType Application)) {
                continue;
            }
            if (& "$cand" -c "(x:=0)") {
                $found = "$cand"
                break
            }
        }
    }
    $found = (Get-Command "$found" -CommandType Application).Source
    $ver = (& $found -c "import sys; print(sys.version)" | Out-String).Trim()
    Write-Debug "Found Python: [$found] (Version $ver)"
    return $found
}
