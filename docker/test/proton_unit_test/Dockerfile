# docker build -t ghcr.io/timeplus-io/proton-unit-test .
ARG FROM_TAG=fae41f7606bb
# https://github.com/timeplus-io/proton-enterprise/issues/5737
FROM clickhouse/unit-test:$FROM_TAG

COPY unit_tests_dbms /
CMD ["/bin/bash", "/run.sh"]