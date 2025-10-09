import sys

# `MD_FLAGS` maps the flag to its bit position.
# The bit positions must match those defined in src/mongoc/mongoc-handshake-private.h
MD_FLAGS = {
    "MONGOC_MD_FLAG_ENABLE_CRYPTO": 0,
    "MONGOC_MD_FLAG_ENABLE_CRYPTO_CNG": 1,
    "MONGOC_MD_FLAG_ENABLE_CRYPTO_COMMON_CRYPTO": 2,
    "MONGOC_MD_FLAG_ENABLE_CRYPTO_LIBCRYPTO": 3,
    "MONGOC_MD_FLAG_ENABLE_CRYPTO_SYSTEM_PROFILE": 4,
    "MONGOC_MD_FLAG_ENABLE_SASL": 5,
    "MONGOC_MD_FLAG_ENABLE_SSL": 6,
    "MONGOC_MD_FLAG_ENABLE_SSL_OPENSSL": 7,
    "MONGOC_MD_FLAG_ENABLE_SSL_SECURE_CHANNEL": 8,
    "MONGOC_MD_FLAG_ENABLE_SSL_SECURE_TRANSPORT": 9,
    "MONGOC_MD_FLAG_EXPERIMENTAL_FEATURES": 10,
    "MONGOC_MD_FLAG_HAVE_SASL_CLIENT_DONE": 11,
    "MONGOC_MD_FLAG_HAVE_WEAK_SYMBOLS": 12,
    "MONGOC_MD_FLAG_NO_AUTOMATIC_GLOBALS": 13,
    "MONGOC_MD_FLAG_ENABLE_SSL_LIBRESSL": 14,
    "MONGOC_MD_FLAG_ENABLE_SASL_CYRUS": 15,
    "MONGOC_MD_FLAG_ENABLE_SASL_SSPI": 16,
    "MONGOC_MD_FLAG_HAVE_SOCKLEN": 17,
    "MONGOC_MD_FLAG_ENABLE_COMPRESSION": 18,
    "MONGOC_MD_FLAG_ENABLE_COMPRESSION_SNAPPY": 19,
    "MONGOC_MD_FLAG_ENABLE_COMPRESSION_ZLIB": 20,
    "MONGOC_MD_FLAG_ENABLE_SASL_GSSAPI": 21,
    "MONGOC_MD_FLAG_ENABLE_RES_NSEARCH": 22,
    "MONGOC_MD_FLAG_ENABLE_RES_NDESTROY": 23,
    "MONGOC_MD_FLAG_ENABLE_RES_NCLOSE": 24,
    "MONGOC_MD_FLAG_ENABLE_RES_SEARCH": 25,
    "MONGOC_MD_FLAG_ENABLE_DNSAPI": 26,
    "MONGOC_MD_FLAG_ENABLE_RDTSCP": 27,
    "MONGOC_MD_FLAG_HAVE_SCHED_GETCPU": 28,
    "MONGOC_MD_FLAG_ENABLE_SHM_COUNTERS": 29,
    "MONGOC_MD_FLAG_TRACE": 30,
    # `MONGOC_MD_FLAG_ENABLE_ICU` was accidentally removed in libmongoc 1.25.0-1.25.3.
    # If parsing a config-bitfield produced by libmongoc 1.25.0-1.25.3, use the version of `parse_handshake_cfg.py` from the git tag 1.25.0.
    "MONGOC_MD_FLAG_ENABLE_ICU": 31,
    "MONGOC_MD_FLAG_ENABLE_CLIENT_SIDE_ENCRYPTION": 32,
    "MONGOC_MD_FLAG_ENABLE_MONGODB_AWS_AUTH": 33,
    "MONGOC_MD_FLAG_ENABLE_SRV": 34,
}

def main():
    flag_to_number = {s: 2 ** i for s,i in MD_FLAGS.items()}

    if len(sys.argv) < 2:
        print ("Usage: python {0} config-bitfield".format(sys.argv[0]))
        print ("Example: python parse_handshake_cfg.py 0x3e65")
        return

    config_bitfield_string = sys.argv[1]
    config_bitfield_num = int(config_bitfield_string, 0)
    print ("Decimal value: {}".format(config_bitfield_num))

    for flag, num in flag_to_number.items():
        v = "true" if config_bitfield_num & num else "false"
        print ("{:<50}: {}".format(flag, v))

if __name__ == "__main__":
    main()
