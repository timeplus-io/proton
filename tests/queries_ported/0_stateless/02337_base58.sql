-- Tags: no-fasttest

SELECT base58_encode('Hold my beer...');
SELECT base58_encode('Hold my beer...', 'Second arg'); -- { serverError 42 }
SELECT base58_decode('Hold my beer...'); -- { serverError 36 }

SELECT base58_decode(encoded) FROM (SELECT base58_encode(val) as encoded FROM (select array_join(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) as val));
SELECT try_base58_decode(encoded) FROM (SELECT base58_encode(val) as encoded FROM (select array_join(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) as val));
SELECT try_base58_decode(val) FROM (SELECT array_join(['Hold my beer', 'Hold another beer', '3csAg9', 'And a wine', 'And another wine', 'And a lemonade', 't1Zv2yaZ', 'And another wine']) as val);

SELECT base58_encode(val) FROM (select array_join(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) as val);
SELECT base58_decode(val) FROM (select array_join(['', '2m', '8o8', 'bQbp', '3csAg9', 'CZJRhmz', 't1Zv2yaZ', '']) as val);

SELECT base58_encode(base58_decode('1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix')) == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';
select base58_encode('\x00\x0b\xe3\xe1\xeb\xa1\x7a\x47\x3f\x89\xb0\xf7\xe8\xe2\x49\x40\xf2\x0a\xeb\x8e\xbc\xa7\x1a\x88\xfd\xe9\x5d\x4b\x83\xb7\x1a\x09') == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';
