FROM ubuntu:20.04

# FIXME, XXX, timeplus deb repo
# ARG repository="deb https://repo.timeplus/deb/stable/ main/"
ARG repository=""
ARG version=1.5.2
ARG gosu_ver=1.10

# set non-empty deb_location_url url to create a docker image
# from debs created by CI build, for example:
# docker build . --network host --build-arg version="21.4.1.6282" --build-arg deb_location_url="https://..." -t ...
ARG deb_location_url=""

# set non-empty single_binary_location_url to create docker image
# from a single binary url (useful for non-standard builds - with sanitizers, for arm64).
# for example (run on aarch64 server):
# docker build . --network host --build-arg single_binary_location_url="https://builds.proton.tech/master/aarch64/proton" -t altinity/proton-server:master-testing-arm
# docker build . --network host --build-arg single_binary_location_url=http://192.168.0.16:8000/build_docker/programs/proton -t ghcr.io/timeplus-io/proton
# note: proton-odbc-bridge is not supported there.
ARG single_binary_location_url=""

# see https://github.com/moby/moby/issues/4032#issuecomment-192327844
ARG DEBIAN_FRONTEND=noninteractive

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.

# To drop privileges, we need 'su' command, that simply changes uid and gid.
# In fact, the 'su' command from Linux is not so simple, due to inherent vulnerability in Linux:
# https://ruderich.org/simon/notes/su-sudo-from-root-tty-hijacking
# It has to mitigate this drawback of Linux, and to do this, 'su' command is creating it's own pseudo-terminal
# and forwarding commands. Due to some ridiculous curcumstances, it does not work in Docker (or it does)
# and for these reasons people are using alternatives to the 'su' command in Docker,
# that don't mess with the terminal, don't care about closing the opened files, etc...
# but can only be safe to drop privileges inside Docker.
# The question - what implementation of 'su' command to use.
# It should be a simple script doing about just two syscalls.
# Some people tend to use 'gosu' tool that is written in Go.
# It is not used for several reasons:
# 1. Dependency on some foreign code in yet another programming language - does not sound alright.
# 2. Anselmo D. Adams suggested not to use it due to false positive alarms in some undisclosed security scanners.

COPY su-exec.c /su-exec.c

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
    && apt-get install -y --no-install-recommends tcc libc-dev && \
        tcc /su-exec.c -o /bin/su-exec && \
        chown root:root /bin/su-exec && \
        chmod 0755 /bin/su-exec && \
        rm /su-exec.c && \
        apt-get purge -y --auto-remove tcc libc-dev libc-dev-bin libc6-dev linux-libc-dev \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && mkdir -p /var/lib/proton /var/log/proton-server /etc/proton-server /etc/proton-client \
    && chmod ugo+Xrw -R /var/lib/proton /var/log/proton-server /etc/proton-server /etc/proton-client

# we need to allow "others" access to proton folder, because docker container
# can be started with arbitrary uid (openshift usecase)

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC

RUN mkdir /docker-entrypoint-initdb.d

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

VOLUME /var/lib/proton

ENV PROTON_CONFIG /etc/proton-server/config.yaml

ENTRYPOINT ["/entrypoint.sh"]
