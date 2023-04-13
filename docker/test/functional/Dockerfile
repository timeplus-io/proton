# docker build --build-arg FROM_TAG=1.2.26 -t timeplus/proton-functional-testrunner .
ARG FROM_TAG=latest
FROM timeplus/proton:$FROM_TAG

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
     pigz

RUN mkdir /datasets \
 && curl https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > /datasets/hits_v1.tsv \
 && curl https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > /datasets/visits_v1.tsv

ENV CLICKHOUSE_USER="proton"
ENV CLICKHOUSE_PASSWORD="proton@t+"
ENV PROTON_SRC="/proton_src"

COPY run_ck_tests.sh /
COPY process_functional_tests_result.py /
COPY flamegraph.pl /

ENTRYPOINT ["/usr/bin/env"]
CMD ["bash", "/run_ck_tests.sh"]
