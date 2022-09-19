select int_div_or_zero(0, 0) = 0;
select int_div_or_zero(-128, -1) = 0;
select int_div_or_zero(-127, -1) = 127;
select int_div_or_zero(1, 1) = 1;
select int_div_or_zero(4, 2) = 2;
