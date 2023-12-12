SELECT number, ip, ip % number FROM (SELECT number, to_ipv4('1.2.3.4') as ip FROM numbers(10, 20));
SELECT bit_shift_left(to_ipv4('192.168.64.15'), 24);
SELECT bit_shift_right(to_ipv4('192.168.64.15'), 24);
SELECT bit_and(to_ipv4('1.0.78.44'), to_ipv4('1.0.78.45'));
SELECT bit_or(to_ipv4('1.0.78.44'), to_ipv4('1.0.78.45'));
