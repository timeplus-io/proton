set query_mode='table';
drop stream if exists ipv6_test26473;

CREATE STREAM ipv6_test26473 (
`ip` string,
`ipv6` ipv6 MATERIALIZED to_ipv6(ip),
`is_ipv6` boolean   MATERIALIZED is_ipv6_string(ip),
`cblock` ipv6   MATERIALIZED cut_ipv6(ipv6, 10, 1)
)
ENGINE = Memory;

insert into ipv6_test26473 values ('2600:1011:b104:a86f:2832:b9c6:6d45:237b');

select ip, ipv6,cblock, cblock1,is_ipv6, cut_ipv6(ipv6, 10, 1) from ipv6_test26473;

drop stream ipv6_test26473;
