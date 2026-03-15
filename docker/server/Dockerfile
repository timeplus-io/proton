FROM python:3.10.12 AS python

FROM ubuntu:22.04

# FIXME, XXX, timeplus deb repo
# ARG repository="deb https://repo.timeplus/deb/stable/ main/"
ARG repository=""
ARG version=3.0.17

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
        python3.10 \
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
ENV SETUPTOOLS_USE_DISTUTILS=stdlib

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

# Copy embedded Python as root (default behavior)
COPY --from=python /usr/local/lib/libpython3.so /usr/share/proton/lib/libpython3.so
COPY --from=python /usr/local/lib/libpython3.10.so.1.0 /usr/share/proton/lib/libpython3.10.so.1.0
COPY --from=python /usr/local/lib/libpython3.10.so /usr/share/proton/lib/libpython3.10.so
COPY --from=python /usr/local/lib/python3.10 /usr/share/proton/lib/python3.10
COPY --from=python /usr/local/bin/python3.10 /usr/share/proton/bin

# Ensure everything under /usr/share/proton is owned by timeplus
RUN chown -R timeplus:timeplus /var/lib/proton /var/log/proton-server /etc/proton-server /etc/proton-client /usr/share/proton/

# Run pip as timeplus so installed files are not root-owned
USER 101:101

# Skip Python package installation on ARM64 since Python UDF is unsupported.
# Also allow opting out via build-arg ENABLE_PYTHON_UDF=0.
RUN arch="$(uname -m)" \
	 && if [ "${ENABLE_PYTHON_UDF}" = "1" ] && ! echo "${arch}" | grep -qiE '^(aarch64|arm64)$'; then \
	      export PYTHONHOME=/usr/share/proton PYTHONNOUSERSITE=1 \
	      && /usr/share/proton/bin/python3.10 -m pip install --no-cache-dir --upgrade --prefix /usr/share/proton pip setuptools wheel truststore \
	      && /usr/share/proton/bin/python3.10 -m pip install --no-cache-dir --timeout 180 --retries 8 --upgrade --prefix /usr/share/proton timeplus-neutrino==0.1.12 \
	      && /usr/share/proton/bin/python3.10 -c 'import importlib.metadata as md, os, re, shutil, sys, sysconfig; from pathlib import Path; sp=Path(sysconfig.get_paths()["purelib"]); paths=sorted(sp.glob("setuptools-*.dist-info")); ver=lambda p: tuple(int(x) for x in re.findall(r"\d+", p.name.removeprefix("setuptools-").removesuffix(".dist-info"))); (len(paths)>1) and [shutil.rmtree(p) for p in paths if p!=max(paths, key=ver)]; vs=sorted({d.version for d in md.distributions() if (d.metadata.get("Name") or "").lower()=="setuptools"}); (len(vs)!=1) and (sys.stderr.write(f"expected exactly one setuptools distribution, got: {vs}\n"), sys.exit(1)); (os.environ.get("SETUPTOOLS_USE_DISTUTILS")=="stdlib") and __import__("setuptools")' \
	    else \
	      echo "Skipping Python package install (ENABLE_PYTHON_UDF=${ENABLE_PYTHON_UDF}, arch=${arch})" ; \
	    fi
VOLUME /var/lib/proton

ENV PROTON_CONFIG /etc/proton-server/config.yaml

ENTRYPOINT ["/entrypoint.sh"]
