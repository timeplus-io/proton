select group_array(a) as b, b[1] from (select (1, 2) as a);
