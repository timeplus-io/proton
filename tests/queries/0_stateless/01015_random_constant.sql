select randConstant() >= 0;
select randConstant() % 10 < 10;
select uniq_exact(x) from (select randConstant() as x);
