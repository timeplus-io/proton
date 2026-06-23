# docker build -t timeplus/proton-unit-test .
ARG FROM_TAG=fae41f7606bb
# https://github.com/timeplus-io/proton-enterprise/issues/5737
FROM clickhouse/unit-test:$FROM_TAG

# Install BOTH Python 3.14 ABIs so the CPythonTest family runs no matter which
# embedded build this image hosts. The amd64 CI workflows build cp314t
# (free-threaded), but a GIL cp314 binary may still be dropped in here;
# PythonInterpreterInfo rejects any ABI-mismatched interpreter, and the base
# image's default python3 is 3.10/3.12 — without the matching binary the whole
# CPythonTest family silently GTEST_SKIPs.
#
# Source: deadsnakes PPA. Chosen over python-build-standalone because
# deadsnakes packages stdlib at /usr/lib/python3.14/ (no 't' suffix).
# Our embedded interpreter (contrib/cpython, built with Py_GIL_DISABLED)
# however resolves the stdlib landmark through getpath.py with
# ABI_THREAD="t", i.e. lib/python3.14t/os.py. Add a symlink so the same
# tree satisfies both lookups; without it Py_Initialize() inside
# unit_tests_dbms aborts with "Failed to import encodings module".
ENV DEBIAN_FRONTEND=noninteractive
# clickhouse/unit-test base ships an apt.kitware.com entry in
# /etc/apt/sources.list whose signing key is expired
# (NO_PUBKEY 65ADECD7A7039392). We don't need cmake from kitware here; strip
# the line so apt-get update doesn't fail on the bad signature.
RUN sed -i '/kitware/d' /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/*kitware* /etc/apt/sources.list.d/*Kitware* \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        software-properties-common gpg-agent ca-certificates \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3.14 python3.14-nogil python3.14-venv \
    && ln -s python3.14 /usr/lib/python3.14t \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install pip + setuptools<81 for both interpreters so CPythonTest.
# RefreshImportStateExtendsNamespacePackagePaths can import pkg_resources
# (setuptools >= 81 removed pkg_resources per PEP 632; the test's legacy
# declare_namespace scenario needs the pre-removal API). Scope: test
# harness only — runtime containers don't depend on pkg_resources.
RUN for py in python3.14t python3.14; do \
        "$py" -m ensurepip --upgrade --default-pip \
        && "$py" -m pip install --no-cache-dir --root-user-action=ignore "setuptools<81" \
        || exit 1; \
    done

COPY unit_tests_dbms /
CMD ["/bin/bash", "/run.sh"]