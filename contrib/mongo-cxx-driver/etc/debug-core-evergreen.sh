#!/usr/bin/env bash

if ! which gdb > /dev/null; then
    echo "gdb not found. Will not search for core files"
    exit 0
fi

echo "Debugging core files"

cd build

shopt -s nullglob
for i in *.core; do
   echo $i
   # find out which executable corresponds to the core dump
   # file <core> produces a string like:
   # ./core: ELF 64-bit LSB core file x86-64, version 1 (SYSV), SVR4-style, from './a.out'
   binary_path=$(file $i | awk '{print $NF}'| awk -F "'" '{print $2}')
   echo "core dump produced by ${binary_path}"
   echo "backtrace full" | gdb -q $binary_path $i
done

cd ..
