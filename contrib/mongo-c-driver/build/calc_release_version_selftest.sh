#!/bin/bash
# calc_release_version_selftest.sh is used to test output of calc_release_version.py.
# run with:
# cd build
# ./calc_release_version_selftest.sh

set -o errexit
set -o pipefail

function assert_eq () {
    a="$1"
    b="$2"
    if [[ "$a" != "$b" ]]; then
        echo "Assertion failed: $a != $b"
        # Print caller
        caller
        exit 1
    fi
}

SAVED_REF=$(git rev-parse HEAD)

function cleanup () {
    [[ -e calc_release_version_test.py ]] && rm calc_release_version_test.py
    git checkout $SAVED_REF --quiet
}

trap cleanup EXIT

: ${PYTHON_INTERP:=python}
if [[ -z $(command -v "${PYTHON_INTERP}") ]]; then
    echo "Python interpreter '${PYTHON_INTERP}' is not valid."
    echo "Set the PYTHON_INTERP environment variable to a valid interpreter."
    exit 1
fi

# copy calc_release_version.py to a separate file not tracked by git so it does not change on `git checkout`
cp calc_release_version.py calc_release_version_test.py

echo "Test a tagged commit ... begin"
{
    git checkout 1.23.4 --quiet
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug)
    assert_eq "$got" "1.23.4"
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug -p)
    assert_eq "$got" "1.23.3"
    git checkout - --quiet
}
echo "Test a tagged commit ... end"

DATE=$(date +%Y%m%d)
echo "Test an untagged commit ... begin"
{
    # 42a818429d6d586a6abf22367ac6fea1e9ce3f2c is commit before 1.23.4
    git checkout 42a818429d6d586a6abf22367ac6fea1e9ce3f2c --quiet
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug)
    assert_eq "$got" "1.23.4-$DATE+git42a818429d"
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug -p)
    assert_eq "$got" "1.23.4"
    git checkout - --quiet
}
echo "Test an untagged commit ... end"

echo "Test next minor version ... begin"
{
    CURRENT_SHORTREF=$(git rev-parse --revs-only --short=10 HEAD)
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug --next-minor)
    # XXX NOTE XXX NOTE XXX
    # If you find yourself looking at this line because the assertion below
    # failed, then it is probably because a new major/minor release was made.
    # Update the expected output to represent the correct next version.
    # XXX NOTE XXX NOTE XXX
    assert_eq "$got" "1.25.0-$DATE+git$CURRENT_SHORTREF"
    got=$("${PYTHON_INTERP}" calc_release_version_test.py --debug --next-minor -p)
    # XXX NOTE XXX NOTE XXX
    # If you find yourself looking at this line because the assertion below
    # failed, then it is probably because a new major/minor release was made.
    # Update the expected output to represent the correct next version.
    # XXX NOTE XXX NOTE XXX
    assert_eq "$got" "1.24.4"
}
echo "Test next minor version ... end"

echo "All tests passed"
