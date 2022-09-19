-- TinyLog
DROP STREAM IF EXISTS nested_01800_tiny_log;
create stream nested_01800_tiny_log (`column` nested(name string, names array(string), types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3)))) ;
INSERT INTO nested_01800_tiny_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_tiny_log FORMAT Null;
DROP STREAM nested_01800_tiny_log;

-- StripeLog
DROP STREAM IF EXISTS nested_01800_stripe_log;
create stream nested_01800_stripe_log (`column` nested(name string, names array(string), types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3)))) ENGINE = StripeLog;
INSERT INTO nested_01800_stripe_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_stripe_log FORMAT Null;
DROP STREAM nested_01800_stripe_log;

-- Log
DROP STREAM IF EXISTS nested_01800_log;
create stream nested_01800_log (`column` nested(name string, names array(string), types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))))  ;
INSERT INTO nested_01800_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_log FORMAT Null;
DROP STREAM nested_01800_log;
