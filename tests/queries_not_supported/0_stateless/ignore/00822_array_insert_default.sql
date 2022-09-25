SELECT sum(ignore(*)) FROM (SELECT array_first(x -> empty(x), [[number]]) FROM numbers(10000000));
