# docker build -t ghcr.io/timeplus-io/proton-unit-test .
ARG FROM_TAG=latest
FROM clickhouse/unit-test:$FROM_TAG

COPY unit_tests_dbms /
CMD ["/bin/bash", "/run.sh"]