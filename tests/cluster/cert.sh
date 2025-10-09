#!/bin/bash

# deployment/clickhouse/
# ├── certs/
# │   ├── ca.crt          # Certificate Authority certificate
# │   ├── ca.key          # Certificate Authority private key
# │   ├── server.crt      # Server certificate
# │   └── server.key      # Server private key
# └── config/
#     ├── client.xml      # Client SSL configuration
#     └── ssl.xml         # Server SSL configuration

CERT_DIR=deployment/clickhouse/certs

run_and_echo() {
  echo "+ $*"
  "$@"
}

function cert_prepare_dir() {
    run_and_echo sudo rm -rf $CERT_DIR
    run_and_echo mkdir -p $CERT_DIR
}

function cert_generate() {
    # Generate CA private key
    run_and_echo openssl genrsa -out ca.key 2048

    # Generate CA certificate
    run_and_echo openssl req -x509 -subj "/CN=ClickHouse CA" -nodes -key ca.key -days 365 -out ca.crt

    # Generate server certificate request with SAN
    run_and_echo openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -addext "subjectAltName = DNS:localhost,DNS:clickhouse,IP:127.0.0.1" \
    -keyout server.key -out server.csr

    # Sign server certificate with CA
    run_and_echo openssl x509 -req -in server.csr -out server.crt \
    -CA ca.crt -CAkey ca.key -days 365 -copy_extensions copy

    run_and_echo mv ca.crt ca.key server.crt server.csr server.key $CERT_DIR
}

function cert_set_permission() {
    run_and_echo chmod 644 $CERT_DIR/server.crt
    run_and_echo chmod 644 $CERT_DIR/server.key
    run_and_echo chmod 644 $CERT_DIR/ca.crt
    run_and_echo sudo chown -R 101:101 $CERT_DIR
}

function cert_exists() {
    files=(
        "$CERT_DIR/ca.crt"
        "$CERT_DIR/ca.key"
        "$CERT_DIR/server.crt"
        "$CERT_DIR/server.csr"
        "$CERT_DIR/server.key"
    )

    for f in "${files[@]}"; do
        if [ ! -f "$f" ]; then
            echo "Error: Certificate file not found: $f"
            exit 1
        fi
    done
    echo "Certificate file all exists!"
}

cert_prepare_dir
cert_generate
cert_set_permission
cert_exists
