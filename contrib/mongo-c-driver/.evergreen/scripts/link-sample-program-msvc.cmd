REM Supported/used environment variables:
REM   LINK_STATIC              Whether to statically link to libmongoc
REM   ENABLE_SSL               Enable SSL with Microsoft Secure Channel
REM   ENABLE_SNAPPY            Enable Snappy compression

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
call "C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\VC\Auxiliary\Build\vcvars64.bat"

cd %BUILD_DIR%
robocopy "%SRCROOT%" "%BUILD_DIR%" /E /XD ".git" "%BUILD_DIR%" "_build" "cmake-build" /NP /NFL /NDL

if "%ENABLE_SNAPPY%"=="1" (
  rem Enable Snappy
  curl -sS --retry 5 -LO https://github.com/google/snappy/archive/1.1.7.tar.gz
  %TAR% xzf 1.1.7.tar.gz
  cd snappy-1.1.7
  %CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% .
  %CMAKE% --build . --target ALL_BUILD --config "Debug" -- /m
  %CMAKE% --build . --target INSTALL --config "Debug" -- /m
  set SNAPPY_OPTION=-DENABLE_SNAPPY=ON
) else (
  set SNAPPY_OPTION=-DENABLE_SNAPPY=OFF
)

cd %BUILD_DIR%
rem Build libmongoc
if "%ENABLE_SSL%"=="1" (
  %CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_PREFIX_PATH=%INSTALL_DIR%\lib\cmake -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DENABLE_SSL=WINDOWS %ENABLE_SNAPPY_OPTION% .
) else (
  %CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_PREFIX_PATH=%INSTALL_DIR%\lib\cmake -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DENABLE_SSL=OFF %ENABLE_SNAPPY_OPTION% .
)

%CMAKE% --build . --target ALL_BUILD --config "Debug" -- /m
%CMAKE% --build . --target INSTALL --config "Debug" -- /m

call ..\.evergreen\scripts\check-installed-files.bat
if errorlevel 1 (
   exit /B %errorlevel%
)

rem Shim library around the DLL.
set SHIM=%INSTALL_DIR%\lib\mongoc-1.0.lib
if not exist %SHIM% (
  echo %SHIM% is missing!
  exit /B 1
) else (
  echo %SHIM% check ok
)

if not exist %INSTALL_DIR%\lib\mongoc-static-1.0.lib (
  echo mongoc-static-1.0.lib missing!
  exit /B 1
) else (
  echo mongoc-static-1.0.lib check ok
)

cd %SRCROOT%

rem Test our CMake package config file with CMake's find_package command.
set EXAMPLE_DIR=%SRCROOT%\src\libmongoc\examples\cmake\find_package

if "%LINK_STATIC%"=="1" (
  set EXAMPLE_DIR="%EXAMPLE_DIR%_static"
)

cd %EXAMPLE_DIR%

if "%ENABLE_SSL%"=="1" (
  cp ..\..\..\tests\x509gen\client.pem .
  cp ..\..\..\tests\x509gen\ca.pem .
  set MONGODB_EXAMPLE_URI="mongodb://localhost/?ssl=true&sslclientcertificatekeyfile=client.pem&sslcertificateauthorityfile=ca.pem&sslallowinvalidhostnames=true"
)

%CMAKE% -G "Visual Studio 15 2017 Win64" -DCMAKE_PREFIX_PATH=%INSTALL_DIR%\lib\cmake .
%CMAKE% --build . --target ALL_BUILD --config "Debug" -- /m

rem Yes, they should've named it "dependencies".
dumpbin.exe /dependents Debug\hello_mongoc.exe

Debug\hello_mongoc.exe %MONGODB_EXAMPLE_URI%
