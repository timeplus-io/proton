SELECT 1 FROM (SELECT array_join(if(empty(range(number)), [1], [2])) from numbers(1));

