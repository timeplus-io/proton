# rebuild in https://github.com/timeplus-io/proton/pull/921 revert in #925
# docker build -t timeplus/proton-binary-builder .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=19

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
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
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
