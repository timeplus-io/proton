REM Supported/used environment variables:
REM    CC             Compiler, "mingw" or "Visual Studio 14 2015 Win64".
REM    BSON_ONLY      Whether to build only the BSON library.

rem Ensure Cygwin executables like sh.exe are not in PATH
rem set PATH=C:\Windows\system32;C:\Windows

echo on
echo

set SRCROOT=%CD%
set TAR=C:\cygwin\bin\tar
set CMAKE=C:\cmake\bin\cmake
set CMAKE_MAKE_PROGRAM=C:\mingw-w64\x86_64-4.9.1-posix-seh-rt_v3-rev1\mingw64\bin\mingw32-make.exe
rem Ensure Cygwin executables like sh.exe are not in PATH
set PATH=C:\cygwin\bin;C:\Windows\system32;C:\Windows;C:\mingw-w64\x86_64-4.9.1-posix-seh-rt_v3-rev1\mingw64\bin;C:\mongoc;src\libbson;src\libmongoc

if "%BSON_ONLY%"=="1" (
   set BUILD_DIR=%CD%\build-dir-bson
   set INSTALL_DIR=%CD%\install-dir-bson
) else (
   set BUILD_DIR=%CD%\build-dir-mongoc
   set INSTALL_DIR=%CD%\install-dir-mongoc
)
rmdir /S /Q %BUILD_DIR%
mkdir %BUILD_DIR%

rmdir /S /Q %INSTALL_DIR%
mkdir %INSTALL_DIR%

set PATH=%PATH%;%INSTALL_DIR%\bin

cd %BUILD_DIR%
robocopy "%SRCROOT%" "%BUILD_DIR%" /E /XD ".git" "%BUILD_DIR%" "_build" "cmake-build" /NP /NFL /NDL

if "%BSON_ONLY%"=="1" (
  set BSON_ONLY_OPTION=-DENABLE_MONGOC=OFF
) else (
  set BSON_ONLY_OPTION=-DENABLE_MONGOC=ON
)

echo.%CC%| findstr /I "gcc">Nul && (
  rem Build libmongoc, with flags that the downstream R driver mongolite uses
  %CMAKE% -G "MinGW Makefiles" -DCMAKE_MAKE_PROGRAM=%CMAKE_MAKE_PROGRAM% -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% -DCMAKE_PREFIX_PATH=%INSTALL_DIR%\lib\cmake %BSON_ONLY_OPTION% .
  %CMAKE% --build .
  if errorlevel 1 (
     exit /B 1
  )
  %CMAKE% --build . --target install
  if errorlevel 1 (
     exit /B 1
  )

  REM no kms-message components should be installed
  if exist %INSTALL_DIR%\include\kms_message\kms_message.h (
     echo kms_message.h found!
     exit /B 1
  ) else (
     echo kms_message.h check ok
  )
  if exist %INSTALL_DIR%\lib\libkms_message-static.a (
     echo libkms_message-static.a found!
     exit /B 1
  ) else (
     echo libkms_message-static.a check ok
  )
  if exist %INSTALL_DIR%\lib\cmake\kms_message\kms_message-config.cmake (
     echo kms_message-config.cmake found!
     exit /B 1
  ) else (
     echo kms_message-config.cmake check ok
  )

  echo > %INSTALL_DIR%\lib\canary.txt

  dir %INSTALL_DIR%\share\mongo-c-driver

  %CMAKE% --build . --target uninstall
  if errorlevel 1 (
     exit /B 1
  )
) || (
  %CMAKE% -G "%CC%" "-DCMAKE_INSTALL_PREFIX=%INSTALL_DIR%" "-DCMAKE_BUILD_TYPE=Debug" %BSON_ONLY_OPTION% .
  %CMAKE% --build . --config Debug
  if errorlevel 1 (
     exit /B 1
  )
  %CMAKE% --build . --config Debug --target install
  if errorlevel 1 (
     exit /B 1
  )

  echo > %INSTALL_DIR%\lib\canary.txt

  REM no kms-message components should be installed
  if exist %INSTALL_DIR%\include\kms_message\kms_message.h (
     echo kms_message.h found!
     exit /B 1
  ) else (
     echo kms_message.h check ok
  )
  if exist %INSTALL_DIR%\lib\libkms_message-static.a (
     echo libkms_message-static.a found!
     exit /B 1
  ) else (
     echo libkms_message-static.a check ok
  )
  if exist %INSTALL_DIR%\lib\cmake\kms_message\kms_message-config.cmake (
     echo kms_message-config.cmake found!
     exit /B 1
  ) else (
     echo kms_message-config.cmake check ok
  )

  dir %INSTALL_DIR%\share\mongo-c-driver

  %CMAKE% --build . --target uninstall
  if errorlevel 1 (
     exit /B 1
  )
)

if exist %INSTALL_DIR%\lib\pkgconfig\libbson-1.0.pc (
   echo libbson-1.0.pc found!
   exit /B 1
) else (
   echo libbson-1.0.pc check ok
)
if exist %INSTALL_DIR%\lib\cmake\bson-1.0\bson-1.0-config.cmake (
   echo bson-1.0-config.cmake found!
   exit /B 1
) else (
   echo bson-1.0-config.cmake check ok
)
if exist %INSTALL_DIR%\lib\cmake\bson-1.0\bson-1.0-config-version.cmake (
   echo bson-1.0-config-version.cmake found!
   exit /B 1
) else (
   echo bson-1.0-config-version.cmake check ok
)
if exist %INSTALL_DIR%\lib\cmake\bson-1.0\bson-targets.cmake (
   echo bson-targets.cmake found!
   exit /B 1
) else (
   echo bson-targets.cmake check ok
)
if not exist %INSTALL_DIR%\lib\canary.txt (
   echo canary.txt not found!
   exit /B 1
) else (
   echo canary.txt check ok
)
if not exist %INSTALL_DIR%\lib (
   echo %INSTALL_DIR%\lib not found!
   exit /B 1
) else (
   echo %INSTALL_DIR%\lib check ok
)
if "%BSON_ONLY%" NEQ "1" (
   if exist %INSTALL_DIR%\lib\pkgconfig\libmongoc-1.0.pc (
     echo libmongoc-1.0.pc found!
     exit /B 1
   ) else (
     echo libmongoc-1.0.pc check ok
   )
   if exist %INSTALL_DIR%\lib\cmake\mongoc-1.0\mongoc-1.0-config.cmake (
     echo mongoc-1.0-config.cmake found!
     exit /B 1
   ) else (
     echo mongoc-1.0-config.cmake check ok
   )
   if exist %INSTALL_DIR%\lib\cmake\mongoc-1.0\mongoc-1.0-config-version.cmake (
     echo mongoc-1.0-config-version.cmake found!
     exit /B 1
   ) else (
     echo mongoc-1.0-config-version.cmake check ok
   )
   if exist %INSTALL_DIR%\lib\cmake\mongoc-1.0\mongoc-targets.cmake (
     echo mongoc-targets.cmake found!
     exit /B 1
   ) else (
     echo mongoc-targets.cmake check ok
   )
)
if exist %INSTALL_DIR%\include\libbson-1.0\bson\bson.h (
   echo bson\bson.h found!
   exit /B 1
) else (
   echo bson\bson.h check ok
)
if exist %INSTALL_DIR%\include\libbson-1.0\bson.h (
   echo bson.h found!
   exit /B 1
) else (
   echo bson.h check ok
)
if exist %INSTALL_DIR%\include\libbson-1.0 (
   echo $INSTALL_DIR\include\libbson-1.0 found!
   exit /B 1
) else (
   echo $INSTALL_DIR\include\libbson-1.0 check ok
)
if "%BSON_ONLY%" NEQ "1" (
   if exist %INSTALL_DIR%\include\libmongoc-1.0\mongoc\mongoc.h (
     echo mongoc\mongoc.h found!
     exit /B 1
   ) else (
     echo mongoc\mongoc.h check ok
   )
   if exist %INSTALL_DIR%\include\libmongoc-1.0\mongoc.h (
     echo mongoc.h found!
     exit /B 1
   ) else (
     echo mongoc.h check ok
   )
   if exist %INSTALL_DIR%\include\libmongoc-1.0 (
     echo $INSTALL_DIR\include\libmongoc-1.0 found!
     exit /B 1
   ) else (
     echo $INSTALL_DIR\include\libmongoc-1.0 check ok
   )
)
if exist %INSTALL_DIR%\share\mongo-c-driver\uninstall-bson.cmd (
   echo uninstall-bson.cmd found!
   exit /B 1
) else (
   echo uninstall-bson.cmd check ok
)
if exist %INSTALL_DIR%\share\mongo-c-driver\uninstall.cmd (
   echo uninstall.cmd found!
   exit /B 1
) else (
   echo uninstall.cmd check ok
)
if exist %INSTALL_DIR%\share\mongo-c-driver\uninstall-bson.sh (
   echo uninstall-bson.sh found!
   exit /B 1
) else (
   echo uninstall-bson.sh check ok
)
if exist %INSTALL_DIR%\share\mongo-c-driver\uninstall.sh (
   echo uninstall.sh found!
   exit /B 1
) else (
   echo uninstall.sh check ok
)
if exist %INSTALL_DIR%\share\mongo-c-driver (
   echo $INSTALL_DIR\share\mongo-c-driver found!
   exit /B 1
) else (
   echo $INSTALL_DIR\share\mongo-c-driver check ok
)
