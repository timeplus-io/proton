@echo off

REM Copyright 2018-present MongoDB, Inc.
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

setlocal EnableExtensions
setlocal EnableDelayedExpansion

set manifest=%1

if not exist "%manifest%" (
   echo.***** Specify location of installation manifest as first parameter >&2
   goto :print_usage
)

set prefix=%2

if "%prefix%"=="" (
   echo.***** Specify installation prefix as second parameter >&2
   goto :print_usage
)

if "%prefix:~1,1%" NEQ ":" (
   echo.***** Installation prefix must refer to an absolute path with drive letter >&2
   goto :print_usage
)

if "%prefix:~-1%" NEQ "\" (
   REM Trailing slash was omitted from prefix, so add it here
   set prefix=%prefix%\
)
REM remove surrounding quotes to prevent fouling string substitution
set prefix=%prefix:"=%

echo.@echo off
echo.
echo.REM MongoDB C++ Driver uninstall program, generated with CMake
echo.
echo.REM Copyright 2018-present MongoDB, Inc.
echo.REM
echo.REM Licensed under the Apache License, Version 2.0 (the "License");
echo.REM you may not use this file except in compliance with the License.
echo.REM You may obtain a copy of the License at
echo.REM
echo.REM   http://www.apache.org/licenses/LICENSE-2.0
echo.REM
echo.REM Unless required by applicable law or agreed to in writing, software
echo.REM distributed under the License is distributed on an "AS IS" BASIS,
echo.REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
echo.REM See the License for the specific language governing permissions and
echo.REM limitations under the License.
echo.
echo.
echo.
echo.REM Windows does not handle a batch script deleting itself during
echo.REM execution.  Copy the uninstall program into the TEMP directory from
echo.REM the environment and run from there so everything in the installation
echo.REM is deleted and the copied program deletes itself at the end.
echo.if /i "%%~dp0" NEQ "%%TEMP%%\" ^(
echo.   copy "%%~f0" "%%TEMP%%\mongoc-%%~nx0" ^>NUL
echo.   "%%TEMP%%\mongoc-%%~nx0" ^& del "%%TEMP%%\mongoc-%%~nx0"
echo.^)
echo.
echo.pushd %prefix%
echo.

for /f "usebackq delims=" %%a in ("%manifest%") do (
   set filepath=%%a
   set suffix=!filepath:%prefix%=!
   set filedir=!suffix:\%%~nxa=!
   call :save_dirs "!filedir!"
   echo.echo Removing file !suffix!
   echo.del !suffix! ^|^| echo ... not removed
)

echo.echo Removing file share\mongo-cxx-driver\uninstall.cmd
echo.del share\mongo-cxx-driver\uninstall.cmd ^|^| echo ... not removed
call :save_dirs "share\mongo-cxx-driver"

for /f "tokens=2 delims=[]" %%a in ('set dirs[') do (
   set directory=%%a
   echo.echo Removing directory !directory!
   echo.^(rmdir !directory! 2^>NUL^) ^|^| echo ... not removed ^(probably not empty^)
)

echo.cd ..
echo.echo Removing top-level installation directory: %prefix%
echo.^(rmdir %prefix% 2^>NUL^) ^|^| echo ... not removed ^(probably not empty^)
echo.
echo.REM Return to the directory from which the program was called
echo.popd

exit /b 0

:print_usage
   echo. >&2
   echo.Usage: >&2
   echo. >&2
   echo.%0 install-manifest install-prefix ^> uninstall.cmd >&2
   echo. >&2
   echo.Note: program prints to standard out; redirect to desired location. >&2
   echo. >&2
   goto :eof

:save_dirs
   set dirlist=%1
   set dirlist=%dirlist:"=%
   set dirs[%dirlist%]=%dirlist%
   REM if the path still has multiple components, strip the last and recurse
   set dirlist=%dirlist%/
   if "%dirlist:\=%" NEQ "%dirlist%" (
      for %%e in (%dirlist:\= %) do set last=%%e
      call :save_dirs "%%dirlist:\!last!=%%"
   )
   goto :end_save_dirs
   :end_save_dirs
