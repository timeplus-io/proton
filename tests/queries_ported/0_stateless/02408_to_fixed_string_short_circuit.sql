select if(number < 0, to_fixed_string(materialize('123'), 2), NULL) from numbers(2);
