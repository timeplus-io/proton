-- Tags: no-fasttest

-- SET allow_experimental_analyzer = 1;

SELECT
    to_ipv4('1.2.3.4') AS ipv4,
    half_md5(ipv4),
    farm_fingerprint64(ipv4),
    xxh3(ipv4),
    wy_hash64(ipv4),
    xx_hash32(ipv4),
    gcc_murmur_hash(ipv4),
    murmur_hash2_32(ipv4),
    java_hash_utf16_le(ipv4),
    int_hash64(ipv4),
    int_hash32(ipv4),
    metro_hash64(ipv4),
    hex(murmur_hash3_128(ipv4)),
    jump_consistent_hash(ipv4, 42),
    sip_hash64(ipv4),
    hex(sip_hash128(ipv4)),
    kostik_consistent_hash(ipv4, 42),
    xx_hash64(ipv4),
    murmur_hash2_64(ipv4),
    city_hash64(ipv4),
    hive_hash(ipv4),
    murmur_hash2_64(ipv4),
    murmur_hash2_32(ipv4),
    kostik_consistent_hash(ipv4,42)
FORMAT Vertical;

SELECT
    to_ipv6('fe80::62:5aff:fed1:daf0') AS ipv6,
    half_md5(ipv6),
    hex(md4(ipv6)),
    hex(md5(ipv6)),
    hex(sha1(ipv6)),
    hex(sha224(ipv6)),
    hex(sha256(ipv6)),
    hex(sha512(ipv6)),
    farm_fingerprint64(ipv6),
    java_hash(ipv6),
    xxh3(ipv6),
    wy_hash64(ipv6),
    xx_hash32(ipv6),
    gcc_murmur_hash(ipv6),
    murmur_hash2_32(ipv6),
    java_hash_utf16_le(ipv6),
    metro_hash64(ipv6),
    hex(sip_hash128(ipv6)),
    hex(murmur_hash3_128(ipv6)),
    sip_hash64(ipv6),
    xx_hash64(ipv6),
    murmur_hash2_64(ipv6),
    city_hash64(ipv6),
    hive_hash(ipv6),
    murmur_hash2_64(ipv6),
    murmur_hash2_32(ipv6)
FORMAT Vertical;
