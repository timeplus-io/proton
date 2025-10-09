-- Tags: no-parallel

DROP DICTIONARY IF EXISTS null_dict;
CREATE DICTIONARY null_dict (
    id              uint64,
    val             uint8,
    default_val     uint8 DEFAULT 123,
    nullable_val    nullable(uint8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);

SELECT
    dict_get('null_dict', 'val', 1337),
    dict_get_or_null('null_dict', 'val', 1337),
    dict_get_or_default('null_dict', 'val', 1337, 111),
    dict_get_uint8('null_dict', 'val', 1337),
    dict_get_uint8_or_default('null_dict', 'val', 1337, 111);

SELECT
    dict_get('null_dict', 'default_val', 1337),
    dict_get_or_null('null_dict', 'default_val', 1337),
    dict_get_or_default('null_dict', 'default_val', 1337, 111),
    dict_get_uint8('null_dict', 'default_val', 1337),
    dict_get_uint8_or_default('null_dict', 'default_val', 1337, 111);

SELECT
    dict_get('null_dict', 'nullable_val', 1337),
    dict_get_or_null('null_dict', 'nullable_val', 1337),
    dict_get_or_default('null_dict', 'nullable_val', 1337, 111);

SELECT val, nullable_val FROM null_dict;

DROP DICTIONARY IF EXISTS null_ip_dict;
CREATE DICTIONARY null_ip_dict (
    network string,
    val     uint8 DEFAULT 77
)
PRIMARY KEY network
SOURCE(NULL())
LAYOUT(IP_TRIE())
LIFETIME(0);

SELECT dict_get('null_ip_dict', 'val', to_ipv4('127.0.0.1'));

SELECT network, val FROM null_ip_dict;
