-- Tags: no-fasttest

 
SELECT base64Encode(val) FROM (select array_join(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) as val);
SELECT base64Decode(val) FROM (select array_join(['', 'Zg==', 'Zm8=', 'Zm9v', 'Zm9vYg==', 'Zm9vYmE=', 'Zm9vYmFy']) as val);
SELECT base64Decode(base64Encode('foo')) = 'foo', base64Encode(base64Decode('Zm9v')) == 'Zm9v';
SELECT tryBase64Decode('Zm9vYmF=Zm9v');
SELECT base64Decode('Zm9vYmF=Zm9v'); -- { serverError 117 }