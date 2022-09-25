DROP STREAM IF EXISTS mean_ztest;
create stream mean_ztest (v int, s uint8) ;
INSERT INTO mean_ztest SELECT number, 0 FROM numbers(100) WHERE number % 2 = 0;
INSERT INTO mean_ztest SELECT number, 1 FROM numbers(100) WHERE number % 2 = 1;
SELECT round_bankers(meanZTest(833.0, 800.0, 0.95)(v, s).1, 16) as z_stat, round_bankers(meanZTest(833.0, 800.0, 0.95)(v, s).2, 16) as p_value, round_bankers(meanZTest(833.0, 800.0, 0.95)(v, s).3, 16) as ci_low, round_bankers(meanZTest(833.0, 800.0, 0.95)(v, s).4, 16) as ci_high FROM mean_ztest;
DROP STREAM IF EXISTS mean_ztest;
