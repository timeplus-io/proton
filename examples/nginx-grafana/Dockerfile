FROM ghcr.io/timeplus-io/proton:latest AS proton
WORKDIR /

# Proton's image: https://github.com/timeplus-io/proton/blob/develop/docker/server/Dockerfile
# is based on Ubuntu:20.04 which uses a libc version that is too old for ipinfo CLI 
# this is why Ubuntu:22.04 is used to build the image
FROM --platform=$BUILDPLATFORM ubuntu:22.04 AS builder
ARG TARGETARCH
ARG PROTON_HOST
ARG IPINFO_KEY

ENV PROTON_HOST ${PROTON_HOST?proton}
ENV IPINFO_KEY ${IPINFO_KEY}

# Copy the Proton binary
COPY --from=proton /usr/bin/proton /usr/bin/

RUN apt-get update && apt-get -y install curl wget libc6 ca-certificates

# Install IPinfo CLI based on architecture
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        curl -LO https://github.com/ipinfo/cli/releases/download/ipinfo-3.3.1/ipinfo_3.3.1_linux_amd64.deb && \
        dpkg -i ipinfo_3.3.1_linux_amd64.deb; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        curl -LO https://github.com/ipinfo/cli/releases/download/ipinfo-3.3.1/ipinfo_3.3.1_linux_arm64.deb && \
        dpkg -i ipinfo_3.3.1_linux_arm64.deb; \
    else \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi

# Write your IPinfo API key in the env var to disk
RUN mkdir -p ~/.config/ipinfo && \
    echo '{"cache_enabled":true,"token":"'$IPINFO_KEY'","open_browser":true}' > ~/.config/ipinfo/config.json

FROM --platform=$TARGETPLATFORM ubuntu:22.04
ARG PROTON_HOST
ARG IPINFO_KEY

ENV PROTON_HOST ${PROTON_HOST?proton}
ENV IPINFO_KEY ${IPINFO_KEY}

COPY --from=builder /usr/bin/proton /usr/bin/
COPY --from=builder /usr/local/bin/ipinfo /usr/local/bin/
COPY --from=builder /root/.config/ipinfo/config.json /root/.config/ipinfo/config.json
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY 01_nginx-access-log.sql /tmp/01_nginx-access-log.sql
COPY 02_csv-export.sql /tmp/02_csv-export.sql
COPY 03_csv-import-01.sql /tmp/03_csv-import-01.sql
COPY 03_csv-import-02.sql /tmp/03_csv-import-02.sql

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
 