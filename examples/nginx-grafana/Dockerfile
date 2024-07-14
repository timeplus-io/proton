FROM ghcr.io/timeplus-io/proton:latest AS proton
WORKDIR /


# Proton's image: https://github.com/timeplus-io/proton/blob/develop/docker/server/Dockerfile
# is based on Ubuntu:20.04 which is too old for ipinfo CLI hence the switch to Ubuntu:22.04
FROM ubuntu:22.04
ARG PROTON_HOST
ARG IPINFO_KEY

ENV PROTON_HOST ${PROTON_HOST?proton}
ENV IPINFO_KEY ${IPINFO_KEY}

# Copy the Proton binary
COPY --from=proton /usr/bin/proton /usr/bin/

RUN apt-get update \
  && apt-get -y install curl wget libc6

# Install IPinfo CLI
RUN curl -LO https://github.com/ipinfo/cli/releases/download/ipinfo-3.3.1/ipinfo_3.3.1_linux_amd64.deb
RUN dpkg -i ipinfo_3.3.1_linux_amd64.deb


# Store your IPinfo API key to disk
RUN mkdir -p ~/.config/ipinfo
RUN cat <<EOF > ~/.config/ipinfo/config.json
{"cache_enabled":true,"token":"$IPINFO_KEY","open_browser":true}
EOF


COPY 01_nginx_access_log.sql /tmp/01_nginx_access_log.sql
COPY 02_csv-export.sql /tmp/02_csv-export.sql
#COPY 03_csv-import.sql /tmp/03_csv-import.sql
COPY 03_csv-import-01.sql /tmp/03_csv-import-01.sql
COPY 03_csv-import-02.sql /tmp/03_csv-import-02.sql

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
 