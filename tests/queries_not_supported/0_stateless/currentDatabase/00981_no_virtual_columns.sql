DROP STREAM IF EXISTS merge_a;
DROP STREAM IF EXISTS merge_b;
DROP STREAM IF EXISTS merge_ab;

create stream merge_a (x uint8) ENGINE = StripeLog;
create stream merge_b (x uint8) ENGINE = StripeLog;
create stream merge_ab AS merge(currentDatabase(), '^merge_[ab]$');

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'merge_ab';

DROP STREAM merge_a;
DROP STREAM merge_b;
DROP STREAM merge_ab;
