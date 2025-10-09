#!/usr/bin/env bash

if [[ "${OSTYPE}" == "cygwin" ]]; then
   echo "Skipping debug-core-evergreen.sh"
   exit
fi

shopt -s nullglob
for i in *.core; do
   echo $i
   echo "backtrace full" | gdb -q ./src/libmongoc/test-libmongoc $i
done

# If there is still a test-libmongoc process running (perhaps due to
# deadlock, or very slow test) attach a debugger and print stacks.
TEST_LIBMONGOC_PID="$(pgrep test-libmongoc)"
if [ -n "$TEST_LIBMONGOC_PID" ]; then
   echo "test-libmongoc processes still running with PID=$TEST_LIBMONGOC_PID"
   echo "backtrace full" | gdb -q -p $TEST_LIBMONGOC_PID
   kill $TEST_LIBMONGOC_PID
fi
