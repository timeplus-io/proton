# Builder stage: produce a free-threaded (PEP 703) CPython 3.14.4 (cp314t)
# to copy into the runtime image. Stock python:3.14.4 is GIL-enabled and
# would be rejected by the embedded interpreter's FT guard, so we use
# deadsnakes. See memory project_cpython_docker_source.md and the PR
# #11852 review thread for why deadsnakes (vs python-build-standalone).
#
# timeplus-neutrino / proton-driver are intentionally NOT pre-installed in this
# runtime: the Timeplus client stack is an on-demand install via
# `SYSTEM INSTALL PYTHON PACKAGE`, and no proton_ci.yml job (unit / smoke /
# stateless / stateful) needs it baked into the image.
FROM ubuntu:22.04 AS python
ENV DEBIAN_FRONTEND=noninteractive
# ENABLE_PYTHON_UDF controls whether this builder stage stages a CPython 3.14
# runtime at all. Defaults to 1; pass --build-arg ENABLE_PYTHON_UDF=0 for
# mini / no-python images (the staging dir is still created so the final
# stage's COPY succeeds, but it is empty and the final RUN no-ops).
#
# ENABLE_PYTHON_FREE_THREADED selects which CPython 3.14 ABI gets staged.
#   1 -> python3.14-nogil  -> binary python3.14t, libpython3.14t.so (FT)
#   0 -> python3.14        -> binary python3.14,  libpython3.14.so  (GIL)
# Must match the embedded interpreter's Py_GIL_DISABLED setting; the
# PythonInterpreterInfo ABI guard now rejects either mismatch direction.
# Default 0 to match docker/packager/packager, where
# --enable-python-free-threaded is opt-in. All Linux amd64 CI workflows
# (proton_ci, nightly_test, manual_trigger_build, release_build) build
# free-threaded and pass --build-arg ENABLE_PYTHON_FREE_THREADED=1; a
# workflow that builds a FT binary MUST pass it or the ABI guard rejects.
ARG ENABLE_PYTHON_UDF=1
ARG ENABLE_PYTHON_FREE_THREADED=0
RUN set -eux; \
    mkdir -p /py-staging/lib /py-staging/bin; \
    if [ "${ENABLE_PYTHON_UDF}" != "1" ]; then \
        echo "ENABLE_PYTHON_UDF=${ENABLE_PYTHON_UDF}; skipping CPython staging."; \
        exit 0; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        software-properties-common gpg-agent ca-certificates dpkg-dev; \
    add-apt-repository -y ppa:deadsnakes/ppa; \
    apt-get update; \
    if [ "${ENABLE_PYTHON_FREE_THREADED}" = "1" ]; then \
        apt-get install -y --no-install-recommends python3.14-nogil python3.14-venv; \
        PY_BIN=python3.14t; PY_LIB=libpython3.14t.so; \
    else \
        apt-get install -y --no-install-recommends python3.14 python3.14-venv; \
        PY_BIN=python3.14; PY_LIB=libpython3.14.so; \
    fi; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    # Resolve the multiarch lib subdir dynamically: x86_64-linux-gnu on
    # amd64, aarch64-linux-gnu on arm64. dpkg-architecture is the
    # canonical source; uname-m fallback covers stripped images.
    MULTIARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null || echo "$(uname -m)-linux-gnu")"; \
    cp "/usr/lib/${MULTIARCH}/${PY_LIB}.1.0" /py-staging/lib/; \
    cp "/usr/lib/${MULTIARCH}/${PY_LIB}"     /py-staging/lib/; \
    cp -r /usr/lib/python3.14 /py-staging/lib/python3.14; \
    cp "/usr/bin/${PY_BIN}" "/py-staging/bin/${PY_BIN}"; \
    # FT-only: embedded interpreter built with Py_GIL_DISABLED makes
    # getpath.c hardcode ABI_THREAD="t" and look for stdlib at
    # lib/python<VER>t/os.py. Symlink so it resolves to the shared stdlib.
    if [ "${ENABLE_PYTHON_FREE_THREADED}" = "1" ]; then \
        ln -s python3.14 /py-staging/lib/python3.14t; \
    fi; \
    # Drop a sentinel naming the staged binary so the final stage's
    # ensurepip step (and the entrypoint's user-site path) can pick the
    # right ABI without re-deriving it.
    echo "${PY_BIN}" > /py-staging/python_binary

FROM ubuntu:22.04

# FIXME, XXX, timeplus deb repo
# ARG repository="deb https://repo.timeplus/deb/stable/ main/"
ARG repository=""
ARG version=3.0.26

# set non-empty deb_location_url url to create a docker image
# from debs created by CI build, for example:
# docker build . --network host --build-arg version="21.4.1.6282" --build-arg deb_location_url="https://..." -t ...
ARG deb_location_url=""

# set non-empty single_binary_location_url to create docker image
# from a single binary url (useful for non-standard builds - with sanitizers, for arm64).
# for example (run on aarch64 server):
# docker build . --network host --build-arg single_binary_location_url="https://builds.timeplu.tech/master/aarch64/timeplus" -t altinity/timeplus-server:master-testing-arm
# docker build . --network host --build-arg single_binary_location_url=http://192.168.0.16:8000/build_docker/programs/proton -t timeplus/proton
# note: proton-odbc-bridge is not supported there.
ARG single_binary_location_url=""
ARG ENABLE_PYTHON_UDF=1

# see https://github.com/moby/moby/issues/4032#issuecomment-192327844
ARG DEBIAN_FRONTEND=noninteractive

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.

RUN groupadd -r timeplus --gid=101 \
    && useradd -r -g timeplus --uid=101 --home-dir=/var/lib/proton --shell=/bin/bash timeplus \
    && echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4 \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        dirmngr \
        gnupg \
        locales \
        wget \
        curl \
        tzdata \
        vim \
        dnsutils \
        sysstat \
        fio \
        iperf \
        dstat \
        tcpdump \
        iftop \
        ethtool \
        iputils-ping \
        traceroute \
        netcat \
        net-tools \
        lsof \
        python3 \
        python3-pip \
    && mkdir -p /etc/apt/sources.list.d \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
    && echo $repository > /etc/apt/sources.list.d/proton.list \
    && if [ -n "$deb_location_url" ]; then \
            echo "installing from custom url with deb packages: $deb_location_url" \
            rm -rf /tmp/proton_debs \
            && mkdir -p /tmp/proton_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/proton-common-static_${version}_amd64.deb" -P /tmp/proton_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/proton-client_${version}_all.deb" -P /tmp/proton_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/proton-server_${version}_all.deb" -P /tmp/proton_debs \
            && dpkg -i /tmp/proton_debs/*.deb ; \
       elif [ -n "$single_binary_location_url" ]; then \
            echo "installing from single binary url: $single_binary_location_url" \
            && rm -rf /tmp/proton_binary \
            && mkdir -p /tmp/proton_binary \
            && wget --progress=bar:force:noscroll "$single_binary_location_url" -O /tmp/proton_binary/proton \
            && chmod +x /tmp/proton_binary/proton \
            && /tmp/proton_binary/proton install --user "timeplus" --group "timeplus" ; \
       else \
           echo "installing from repository: $repository" \
           && apt-get update \
           && apt-get --yes -o "Dpkg::Options::=--force-confdef" -o "Dpkg::Options::=--force-confold" upgrade \
           && apt-get install --allow-unauthenticated --yes --no-install-recommends \
                proton-common-static=$version \
                proton-client=$version \
                proton-server=$version ; \
       fi \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && mkdir -p /var/lib/proton /var/log/proton-server /etc/proton-server /etc/proton-client

# we need to allow "others" access to proton folder, because docker container
# can be started with arbitrary uid (openshift usecase)

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC

RUN mkdir /docker-entrypoint-initdb.d \
 && mkdir -p /usr/share/proton

COPY resources/protos /usr/share/proton/protos/

# The proton-enterprise will read listen_host from node_descriptor.
# Internally, we set node.listen_host to an empty value to minimize changes.
# Consider the docker for ipv6 support is by default turned off https://docs.docker.com/config/daemon/ipv6/
COPY docker_related_config.xml /etc/proton-server/config.d/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && mkdir -p /usr/share/proton/bin \
    && mkdir -p /usr/share/proton/lib

# Copy CPython 3.14 runtime from the builder stage. The builder normalized
# everything into /py-staging/, abstracting over (a) the multiarch lib
# subdirectory (x86_64-linux-gnu vs aarch64-linux-gnu) and (b) the FT-vs-GIL
# ABI selection. This single COPY pair works for amd64 + arm64 and for
# either ABI; the embedded build's PythonInterpreterInfo will refuse to
# start if the staged binary's ABI does not match the embedded interpreter.
#
# When ENABLE_PYTHON_UDF=0 the python stage staged an empty dir, so the
# COPY is effectively a no-op directory and the RUN below skips ensurepip.
#
# Defaults match docker/packager/packager: ENABLE_PYTHON_UDF=1,
# ENABLE_PYTHON_FREE_THREADED=0 (FT is opt-in there too). FT builds must
# pass --build-arg ENABLE_PYTHON_FREE_THREADED=1 to both the builder image
# and this image so the embedded binary and runtime libs agree.
ARG ENABLE_PYTHON_UDF=1
ARG ENABLE_PYTHON_FREE_THREADED=0
COPY --from=python /py-staging /py-staging-src
RUN set -eux; \
    if [ "${ENABLE_PYTHON_UDF}" = "1" ]; then \
        cp -a /py-staging-src/lib/. /usr/share/proton/lib/; \
        cp -a /py-staging-src/bin/. /usr/share/proton/bin/; \
        # Preserve the binary-name marker so entrypoint.sh can pick the
        # matching user-site path at container start without re-deriving
        # the ABI. Lives under /usr/share/proton so it travels with
        # the rest of the runtime and survives volume mounts on /var.
        install -m 0644 /py-staging-src/python_binary /usr/share/proton/python_binary; \
        PY_BIN="$(cat /usr/share/proton/python_binary)"; \
        # Install pip into the embedded interpreter's site-packages so that
        # SYSTEM INSTALL PYTHON PACKAGE (which shells out to `<PY_BIN> -m pip
        # install --user`) can find it. deadsnakes ships ensurepip's bundled
        # pip wheel at lib/python3.14/ensurepip/_bundled/ but never installs
        # it; running under the embedded-layout binary places pip at
        # /usr/share/proton/local/lib/<PY_BIN_TAG>/dist-packages/
        # (Debian's posix_local scheme), which the binary discovers via
        # sys.path.
        "/usr/share/proton/bin/${PY_BIN}" -m ensurepip --upgrade --default-pip; \
        "/usr/share/proton/bin/${PY_BIN}" -m pip --version; \
    fi; \
    rm -rf /py-staging-src

# Ensure everything under /usr/share/proton is owned by timeplus
RUN chown -R timeplus:timeplus /var/lib/proton /var/log/proton-server /etc/proton-server /etc/proton-client /usr/share/proton/

USER 101:101
VOLUME /var/lib/proton

ENV PROTON_CONFIG /etc/proton-server/config.yaml

ENTRYPOINT ["/entrypoint.sh"]
