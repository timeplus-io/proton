# docker build -t timeplus/cctools .

# This is a hack to significantly reduce the build time of the timeplus/binary-builder
# It's based on the assumption that we don't care of the cctools version so much
# It event does not depend on the timeplus/fasttest in the `docker/images.json`
FROM ubuntu:20.04 AS builder

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
ARG LLVM_VERSION=17
ARG LIBTAPI_COMMIT=15dfc2a8c9a2a89d06ff227560a69f5265b692f9
ARG CCTOOLS_COMMIT=2a3e1c2a6ff54a30f898b70cfb9ba1692a55fad7

RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive

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
        clang-${LLVM_VERSION} \
        cmake \
        fakeroot \
        zlib1g-dev \
        git \
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

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# Build and install libtapi (required for cctools)
RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout ${LIBTAPI_COMMIT} \
    && INSTALLPREFIX=/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin (x86-64 and aarch64)
RUN git clone https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && git checkout ${CCTOOLS_COMMIT} \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=x86_64-apple-darwin \
    && make install -j$(nproc) \
    && make clean \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=aarch64-apple-darwin \
    && make install -j$(nproc) \
    && cd ../.. \
    && rm -rf cctools-port

# Verify the installation by checking if key binaries exist
RUN ls -la /cctools/bin/x86_64-apple-darwin-* && \
    ls -la /cctools/bin/aarch64-apple-darwin-* && \
    echo "Verification complete: cctools built successfully"

# Final stage: copy only the cctools directory
FROM scratch
COPY --from=builder /cctools /cctools
