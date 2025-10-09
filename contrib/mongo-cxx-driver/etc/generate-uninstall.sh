#!/bin/sh

# Copyright 2018-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

print_usage() {
   echo
   echo "Usage:"
   echo
   echo "${0} install-manifest install-prefix > uninstall.sh"
   echo
   echo "Note: program prints to standard out; redirect to desired location."
   echo
} >&2

manifest="${1}"

if [ ! -e "${manifest}" ]; then
   echo "***** Specify location of installation manifest as first parameter" >&2
   print_usage
   exit 1
fi

prefix="${2}"

if [ -z "${prefix}" ]; then
   echo "***** Specify installation prefix as second parameter" >&2
   print_usage
   exit 1
fi

if [ "${prefix}" = "${prefix#/}" ]; then
   echo "***** Installation prefix must refer to an absolute path" >&2
   print_usage
   exit 1
fi

if [ "${prefix}" = "${prefix%/}" ]; then
   # Trailing slash was omitted from prefix, so add it here
   prefix="${prefix}/"
fi

# If a DESTDIR is specified, ensure it ends with a trailing slash.
if [ "${DESTDIR}" ]; then
   if [ "${DESTDIR}" = "${DESTDIR%/}" ]; then
      # Trailing slash was omitted, so add it here
      DESTDIR="${DESTDIR}/"
   fi
fi


printf "#!/bin/sh\n"
printf "# MongoDB C++ Driver uninstall program, generated with CMake"
printf "\n"
printf "# Copyright 2018-present MongoDB, Inc.\n"
printf "#\n"
printf "# Licensed under the Apache License, Version 2.0 (the \"License\");\n"
printf "# you may not use this file except in compliance with the License.\n"
printf "# You may obtain a copy of the License at\n"
printf "#\n"
printf "#   http://www.apache.org/licenses/LICENSE-2.0\n"
printf "#\n"
printf "# Unless required by applicable law or agreed to in writing, software\n"
printf "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
printf "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
printf "# See the License for the specific language governing permissions and\n"
printf "# limitations under the License.\n"
printf "\n"
printf "save_pwd=\$(pwd)\n"
printf "cd %s\n" "${DESTDIR}${prefix}"
printf "\n"

dirs=
while IFS= read -r line || [ -n "${line}" ]; do
   suffix="${line#${prefix}}"
   dir="$(dirname "${suffix}")"
   while [ "${dir}" != "." ]; do
      dirs="${dirs}${dir}\n"
      dir="$(dirname "${dir}")"
   done
   printf "printf \"Removing file \\\"%s\\\"\"\n" "${suffix}"
   printf "(rm -f \"%s\" && printf \"\\\n\") || printf \" ... not removed\\\n\"\n" "${suffix}"
done < "${manifest}"

printf "printf \"Removing file \\\"share/mongo-cxx-driver/uninstall.sh\\\"\"\n"
printf "(rm -f \"share/mongo-cxx-driver/uninstall.sh\" && printf \"\\\n\") || printf \" ... not removed\\\n\"\n"
dirs="${dirs}share/mongo-cxx-driver\nshare\n"

echo "${dirs}" | sort -ru | while IFS= read -r dir; do
   if [ -n "${dir}" ]; then
      printf "printf \"Removing directory \\\"%s\\\"\"\n" "${dir}"
      printf "(rmdir \"%s\" 2>/dev/null && printf \"\\\n\") || printf \" ... not removed (probably not empty)\\\n\"\n" "${dir}"
   fi
done

printf "cd ..\n"
printf "printf \"Removing top-level installation directory: \\\"%s\\\"\"\n" "${DESTDIR}${prefix}"
printf "(rmdir \"%s\" 2>/dev/null && printf \"\\\n\") || printf \" ... not removed (probably not empty)\\\n\"\n" "${DESTDIR}${prefix}"
printf "\n"
printf "# Return to the directory from which the program was called\n"
printf "cd \${save_pwd}\n"
