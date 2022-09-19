SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(int_div(number, 54321)) AS nums, group_array(to_string(int_div(number, 98765)))) AS arr FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY intHash32(number) % 100000);

SELECT array_enumerate_uniq([[1], [2], [34], [1]]);
SELECT array_enumerate_uniq([(1, 2), (3, 4), (1, 2)]);

