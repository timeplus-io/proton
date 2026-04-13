# rebuild in https://github.com/timeplus-io/proton-enterprise/pull/7825
# docs: https://github.com/timeplus-io/proton-enterprise/wiki/How-to-generate-proton%E2%80%90binary%E2%80%90builder-image
# docker build -t timeplus/proton-binary-builder .
# NOTE: `clickhouse/cctools:*` binaries require newer glibc/libstdc++ than Ubuntu 20.04 provides.
# Ubuntu 22.04+ is needed for Darwin cross-linking.
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=21

RUN apt-get update \
    && apt-get install \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        wget \
        gpg \
        curl \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="5ffc7c9a9299ce774f81cada703e23ebba5bdfb0345b6c3b667b3ead7aa21c75ef62ccd74f7a8f2aa0cbe158d3068bbe" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg \
    && echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*


# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        build-essential \
        ccache \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        cmake \
        fakeroot \
        gdb \
        zlib1g-dev \
        git \
        gperf \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        llvm-${LLVM_VERSION}-dev \
        libclang-rt-${LLVM_VERSION}-dev \
        moreutils \
        ninja-build \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        automake \
        m4 \
        texinfo \
        libtool \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

# This symlink is required by packager script
RUN ln -s /usr/bin/clang-tidy-${LLVM_VERSION} /usr/bin/clang-tidy

# python udf require
RUN ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-tblgen /usr/bin/llvm-tblgen
RUN ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-profdata /usr/bin/llvm-profdata

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# Copy pre-built cctools from the clickhouse/cctools image
COPY --from=clickhouse/cctools:d9e3596e706b /cctools /cctools

# Download toolchain and SDK for Darwin
RUN curl -sL -O https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz


RUN wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.71.tar.gz \
    && tar xvf autoconf-2.71.tar.gz \
    && cd autoconf-2.71 \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && rm -rf autoconf-2.71 autoconf-2.71.tar.gz

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1 | ts"]
