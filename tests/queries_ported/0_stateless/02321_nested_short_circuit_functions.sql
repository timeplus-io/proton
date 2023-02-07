select number >= 0 and if(number != 0, int_div(1, number), 1) from numbers(5);
select if(number >= 0, if(number != 0, int_div(1, number), 1), 1) from numbers(5);
select number >= 0 and if(number = 0, 0, if(number == 1, int_div(1, number), if(number == 2, int_div(1, number - 1), if(number == 3, int_div(1, number - 2), int_div(1, number - 3))))) from numbers(10);
