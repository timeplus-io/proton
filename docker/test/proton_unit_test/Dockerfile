# docker build -t ghcr.io/timeplus-io/proton-unit-test .
ARG FROM_TAG=fae41f7606bb
FROM clickhouse/unit-test:$FROM_TAG

COPY unit_tests_dbms /
CMD ["/bin/bash", "/run.sh"]