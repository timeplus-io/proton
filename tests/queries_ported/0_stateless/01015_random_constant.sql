select rand_constant() >= 0;
select rand_constant() % 10 < 10;
select uniq_exact(x) from (select rand_constant() as x);
