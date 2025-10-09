#!/bin/bash
set -e

log () {
  echo "$@" 1>&2
}

print_error () {
  echo "$@" 1>&2
  exit 1
}

print_usage () {
  print_error "Usage: gen-ssl-cert-key <fqdn> <output-dir>"
}

gen_cert_subject () {
  local fqdn="$1"
  [[ "${fqdn}" != "" ]] || print_error "FQDN cannot be blank"
  echo "/C=XX/ST=X/O=X/localityName=X/CN=${fqdn}/organizationalUnitName=X/emailAddress=X/"
}

main () {
  local fqdn="$1"
  local sslDir="$2"
  [[ "${fqdn}" != "" ]] || print_usage
  [[ -d "${sslDir}" ]] || print_error "Directory does not exist: ${sslDir}"

  local caCertFile="${sslDir}/ca.crt"
  local caKeyFile="${sslDir}/ca.key"
  local certFile="${sslDir}/server.crt"
  local certShaFile="${sslDir}/server-cert.sha1"
  local keyFile="${sslDir}/server.key"
  local csrFile=$(mktemp)
  local clientCertFile="${sslDir}/client-cert.pem"
  local clientKeyFile="${sslDir}/client-key.pem"
  local clientEncryptedKeyFile="${sslDir}/client-key-enc.pem"
  local clientKeystoreFile="${sslDir}/client-keystore.jks"
  local fullClientKeystoreFile="${sslDir}/fullclient-keystore.jks"
  local tmpKeystoreFile=$(mktemp)
  local pcks12FullKeystoreFile="${sslDir}/fullclient-keystore.p12"
  local clientReqFile=$(mktemp)

  log "Generating CA key"
  openssl genrsa -out "${caKeyFile}" 2048

  log "Generating CA certificate"
  openssl req \
    -sha1 \
    -new \
    -x509 \
    -nodes \
    -days 3650 \
    -subj "$(gen_cert_subject ca.example.com)" \
    -key "${caKeyFile}" \
    -out "${caCertFile}"

  log "Generating private key"
  openssl genrsa -out "${keyFile}" 2048

  log "Generating certificate signing request"
  openssl req \
    -new \
    -batch \
    -sha1 \
    -subj "$(gen_cert_subject "$fqdn")" \
    -set_serial 01 \
    -key "${keyFile}" \
    -out "${csrFile}" \
    -nodes

  log "Generating X509 certificate"
  openssl x509 \
    -req \
    -sha1 \
    -set_serial 01 \
    -CA "${caCertFile}" \
    -CAkey "${caKeyFile}" \
    -days 3650 \
    -in "${csrFile}" \
    -signkey "${keyFile}" \
    -out "${certFile}"

  log "Generating client certificate"
  openssl req \
    -batch \
    -newkey rsa:2048 \
    -days 3600 \
    -subj "$(gen_cert_subject "$fqdn")" \
    -nodes \
    -keyout "${clientKeyFile}" \
    -out "${clientReqFile}"

  log "Generating password protected client key file"
  openssl rsa \
     -aes256 \
     -in "${clientKeyFile}" \
     -out "${clientEncryptedKeyFile}" \
     -passout pass:qwerty

   log "Generating finger print of server certificate"
   openssl x509 \
     -noout \
     -fingerprint \
     -sha1 \
     -inform pem \
     -in "${certFile}" | \
     sed -e  "s/SHA1 Fingerprint=//g" \
     > "${certShaFile}"

  log "copy ca file"
    cp "${caCertFile}" "${sslDir}/cacert.pem"

  openssl x509 \
    -req \
    -in "${clientReqFile}" \
    -days 3600 \
    -CA "${caCertFile}" \
    -CAkey "${caKeyFile}" \
    -set_serial 01 \
    -out "${clientCertFile}"

  # Now generate a keystore with the client cert & key
  log "Generating client keystore"
  openssl pkcs12 \
    -export \
    -in "${clientCertFile}" \
    -inkey "${clientKeyFile}" \
    -out "${tmpKeystoreFile}" \
    -name "mysqlAlias" \
    -passout pass:kspass


  # Now generate a full keystore with the client cert & key + trust certificates
  log "Generating full client keystore"
  openssl pkcs12 \
    -export \
    -in "${clientCertFile}" \
    -inkey "${clientKeyFile}" \
    -out "${pcks12FullKeystoreFile}" \
    -name "mysqlAlias" \
    -passout pass:kspass


  # Clean up CSR file:
  rm "$csrFile"
  rm "$clientReqFile"
  rm "$tmpKeystoreFile"

  log "Generated key file and certificate in: ${sslDir}"
  ls -l "${sslDir}"
}

main "$@"

