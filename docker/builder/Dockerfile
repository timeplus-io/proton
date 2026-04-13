FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=21

# RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="5ffc7c9a9299ce774f81cada703e23ebba5bdfb0345b6c3b667b3ead7aa21c75ef62ccd74f7a8f2aa0cbe158d3068bbe" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list

RUN apt-get update \
    && apt-get install \
        bash \
        ccache \
        cmake \
        curl \
        expect \
        g++ \
        gcc \
        git \
        ninja-build \
        perl \
        pkg-config \
        python3 \
        python3-lxml \
        python3-requests \
        python3-termcolor \
        tzdata \
        llvm-${LLVM_VERSION} \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        lld-${LLVM_VERSION} \
        lldb-${LLVM_VERSION} \
        --yes --no-install-recommends

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
