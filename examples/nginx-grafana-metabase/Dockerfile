FROM ghcr.io/timeplus-io/proton:latest

ARG PROTON_HOST

ENV PROTON_HOST ${PROTON_HOST?proton}

COPY 01_nginx_access_log.sql /tmp/01_nginx_access_log.sql

# pause otherwise it fails with "Code: 210. DB::NetException: Connection refused (proton:8463). (NETWORK_ERROR)"
ENTRYPOINT sleep 10 && proton client --host $PROTON_HOST --multiquery < /tmp/01_nginx_access_log.sql


 