#!/usr/bin/env bash

# regex to match public headers.
declare pattern
pattern="\.\/src\/libmongoc\/src\/mongoc\/mongoc.*[^private]\.h$"

# public headers we expect not to have BSON_BEGIN_DECLS and BSON_END_DECLS.
declare exclude
exclude="\.\/src\/libmongoc\/src\/mongoc\/mongoc-macros.h|.\/src\/libmongoc\/src\/mongoc\/mongoc.h"

# get all public headers.
find ./src/libmongoc/src/mongoc -regex $pattern -regextype posix-extended -not -regex $exclude | sort >/tmp/public_headers.txt

# get all public headers with BSON_BEGIN_DECLS.
find ./src/libmongoc/src/mongoc -regex $pattern -regextype posix-extended -not -regex $exclude | xargs grep -l "BSON_BEGIN_DECLS" | sort >/tmp/public_headers_with_extern_c.txt

echo "checking if any public headers are missing 'extern C' declaration"

# check if there's any diff.
diff -y /tmp/public_headers.txt /tmp/public_headers_with_extern_c.txt

# use return status of diff
exit $?
