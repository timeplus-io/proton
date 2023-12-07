drop stream if exists geoip_t;

create stream if not exists geoip_t (
        cidr string,
        latitude float64,
        longitude float64,
        country_code string,
        state string,
        city string
) engine = MergeTree primary key cidr;

insert into geoip_t values ('188.166.84.125',52.3759,4.8975,'NL','North Holland','Amsterdam');

drop dictionary if exists geoip;

CREATE DICTIONARY geoip (
        cidr string,
        latitude float64,
        longitude float64,
        country_code string,
        state string,
        city string
) PRIMARY KEY cidr
SOURCE (ClickHouse (table 'geoip_t' user 'proton' password 'proton@t+' ))
LIFETIME (MIN 300 MAX 360)
LAYOUT (IP_TRIE());

select dict_get('geoip', 'latitude', to_ipv4('188.166.84.125'));
select dict_get_or_default('geoip', 'latitude', to_ipv4('188.166.84.111'), 1.0);
