REM Supported/used environment variables:
REM   LINK_STATIC              Whether to statically link to libbson

rem Ensure Cygwin executables like sh.exe are not in PATH
rem set PATH=C:\Windows\system32;C:\Windows

echo on
echo

set TAR=C:\cygwin\bin\tar

set SRCROOT=%CD%
set BUILD_DIR=%CD%\build-dir
rmdir /S /Q %BUILD_DIR%
mkdir %BUILD_DIR%

set INSTALL_DIR=%CD%\install-dir
rmdir /S /Q %INSTALL_DIR%
mkdir %INSTALL_DIR%

set PATH=%PATH%;%INSTALL_DIR%\bin
rem Set path to dumpbin.exe and other VS tools.
call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\VC\Auxiliary\Build\vcvars64.bat"

cd %BUILD_DIR%
robocopy "%SRCROOT%" "%BUILD_DIR%" /E /XD ".git" "%BUILD_DIR%" "_build" "cmake-build" /NP /NFL /NDL

if "%LINK_STATIC%"=="1" (
  %CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DENABLE_TESTS=OFF .
) else (
  %CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DENABLE_TESTS=OFF -DENABLE_STATIC=OFF .
)

%CMAKE% --build . --target ALL_BUILD --config "Debug" -- /m
%CMAKE% --build . --target INSTALL --config "Debug" -- /m

call ..\.evergreen\scripts\check-installed-files-bson.bat
if errorlevel 1 (
   exit /B %errorlevel%
)

rem Test our CMake package config file with CMake's find_package command.
set EXAMPLE_DIR=%SRCROOT%\src\libbson\examples\cmake\find_package

if "%LINK_STATIC%"=="1" (
  set EXAMPLE_DIR="%EXAMPLE_DIR%_static"
)

cd %EXAMPLE_DIR%
%CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_PREFIX_PATH=%INSTALL_DIR%\lib\cmake .
%CMAKE% --build . --target ALL_BUILD --config "Debug" -- /m

rem Yes, they should've named it "dependencies".
dumpbin.exe /dependents Debug\hello_bson.exe

Debug\hello_bson.exe
