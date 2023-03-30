SELECT to_fixed_string('aa' , 2 ) as a, concat(a, a);
SELECT to_fixed_string('aa' , 2 ) as a, length(concat(a, a));
SELECT to_fixed_string('aa' , 2 ) as a, to_type_name(concat(a, a));
