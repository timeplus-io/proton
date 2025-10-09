#!/bin/sh

# Make HTML page of all man pages so we can see them in Evergreen.
#
# Command-line positional parameters:
#
#     LIBNAME     bson or mongoc, for the page title
#     DIRECTORY   path to .3 files
#

# Is the "aha" program in CWD or installed systemwide?
if [ -e "aha" ]; then
  AHA="./aha"
else
  AHA="aha"
fi

echo "
<html>
  <head>
    <meta charset="utf-8">
    <title>$1 man pages</title>
    <style type="text/css">
      pre, div {
        margin: 2em;
        clear: both;
        float: left;
      }

      hr {
        width: 51em;
        font-family: monospace;
        clear: both;
        float: left;
      }
    </style>
  </head>
  <body><pre>"

for doc in $2/*.3; do
  fullpath=`pwd`/$doc
  name=$(basename $doc)

  if [ ! -e "$fullpath" ]; then
    >&2 echo "No .3 files in $2!"
    exit 1
  fi

  echo "<div>$name</div><hr><pre>"

  # suppress man's warnings "cannot break line" or "cannot adjust line"
  MAN_KEEP_FORMATTING=1 COLUMNS=80 man "$fullpath" 2>/dev/null | ul -t xterm | "$AHA" --no-header

  echo "</pre>
  <hr>"
done

echo "</pre></body></html>"
