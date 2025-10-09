# docker build --build-arg FROM_TAG=1.2.26 --build-arg FROM_REPO=$PROTON_REPO -t timeplus/proton-functional-testrunner .
ARG FROM_TAG=latest
ARG FROM_REPO=timeplus/proton
FROM $FROM_REPO:$FROM_TAG

# Switch to root to install packagtes.
USER 0:0

RUN apt-get update \
 && env DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
     moreutils \
     xz-utils \
     python3 \
     python3-lxml \
     python3-requests \
     python3-termcolor \
     python3-pip \
     curl \
     jq \
     pigz

COPY datasets /datasets

RUN unxz /datasets/hits_v1.tsv.xz --threads=`nproc` \
 && unxz /datasets/visits_v1.tsv.xz --threads=`nproc` \
 && mkdir -p /test_output \
 && chown timeplus:timeplus -R /datasets /test_output

# Switch back to timeplus user
USER 101:101

ENV CLICKHOUSE_USER="proton"
ENV CLICKHOUSE_PASSWORD="proton@t+"
ENV PROTON_SRC="/proton_src"

COPY run_ck_tests.sh /
COPY process_functional_tests_result.py /
COPY flamegraph.pl /

ENTRYPOINT ["/usr/bin/env"]
CMD ["bash", "/run_ck_tests.sh"]
