echo on
echo

set INSTALL_DIR=%CD%\build\install

echo > %INSTALL_DIR%\lib\canary.txt

dir %INSTALL_DIR%\share\mongo-cxx-driver

set CMAKE="C:\cmake\bin\cmake.exe"

pushd build
%CMAKE% --build . --target uninstall
if errorlevel 1 (
  exit /B 1
)
popd

dir %INSTALL_DIR% /s

if exist %INSTALL_DIR%\lib\pkgconfig\libbsoncxx.pc (
  echo libbsoncxx.pc found!
  exit /B 1
) else (
  echo libbsoncxx.pc check ok
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
if exist %INSTALL_DIR%\lib\pkgconfig\libmongocxx.pc (
  echo libmongocxx.pc found!
  exit /B 1
) else (
  echo libmongocxx.pc check ok
)
if exist %INSTALL_DIR%\include\bsoncxx\v_noabi\bsoncxx\json.hpp (
  echo bsoncxx\json.hpp found!
  exit /B 1
) else (
  echo bsoncxx\json.hpp check ok
)
if exist %INSTALL_DIR%\include\bsoncxx\v_noabi\bsoncxx\types.hpp (
  echo bsoncxx\types.hpp found!
  exit /B 1
) else (
  echo bsoncxx\types.hpp check ok
)
if exist %INSTALL_DIR%\include\bsoncxx (
  echo $INSTALL_DIR\include\bsoncxx found!
  exit /B 1
) else (
  echo $INSTALL_DIR\include\bsoncxx check ok
)
if exist %INSTALL_DIR%\include\mongocxx\v_noabi\mongocxx\hint.hpp (
  echo mongocxx\hint.hpp found!
  exit /B 1
) else (
  echo mongocxx\hint.hpp check ok
)
if exist %INSTALL_DIR%\include\mongocxx\v_noabi\mongocxx\logger.hpp (
  echo mongocxx\logger.hpp found!
  exit /B 1
) else (
  echo mongocxx\logger.hpp check ok
)
if exist %INSTALL_DIR%\include\mongocxx (
  echo $INSTALL_DIR\include\mongocxx found!
  exit /B 1
) else (
  echo $INSTALL_DIR\include\mongocxx check ok
)
if exist %INSTALL_DIR%\share\mongo-cxx-driver\uninstall.cmd (
  echo uninstall.cmd found!
  exit /B 1
) else (
  echo uninstall.cmd check ok
)
if exist %INSTALL_DIR%\share\mongo-cxx-driver\uninstall.sh (
  echo uninstall.sh found!
  exit /B 1
) else (
  echo uninstall.sh check ok
)
if exist %INSTALL_DIR%\share\mongo-cxx-driver (
  echo $INSTALL_DIR\share\mongo-cxx-driver found!
  exit /B 1
) else (
  echo $INSTALL_DIR\share\mongo-cxx-driver check ok
)
