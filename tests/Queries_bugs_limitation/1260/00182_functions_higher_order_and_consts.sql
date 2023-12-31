SELECT '---map--';
SELECT array_map(x -> 123, empty_array_uint8());
SELECT array_map(x -> 123, [1, 2, 3]);
SELECT array_map(x -> 123, range(number)) FROM system.numbers LIMIT 10;
SELECT '---filter--';
SELECT array_filter(x -> 0, empty_array_uint8());
SELECT array_filter(x -> 0, [1, 2, 3]);
SELECT array_filter(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> 1, empty_array_uint8());
SELECT array_filter(x -> 1, [1, 2, 3]);
SELECT array_filter(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---count---';
SELECT array_count(x -> 0, empty_array_uint8());
SELECT array_count(x -> 0, [1, 2, 3]);
SELECT array_count(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_count(x -> 1, empty_array_uint8());
SELECT array_count(x -> 1, [1, 2, 3]);
SELECT array_count(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---sum---';
SELECT array_sum(x -> 0, empty_array_uint8());
SELECT array_sum(x -> 0, [1, 2, 3]);
SELECT array_sum(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> 10, empty_array_uint8());
SELECT array_sum(x -> 10, [1, 2, 3]);
SELECT array_sum(x -> 10, range(number)) FROM system.numbers LIMIT 10;
SELECT '---all---';
SELECT array_all(x -> 0, empty_array_uint8());
SELECT array_all(x -> 0, [1, 2, 3]);
SELECT array_all(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_all(x -> 1, empty_array_uint8());
SELECT array_all(x -> 1, [1, 2, 3]);
SELECT array_all(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---exists---';
SELECT array_exists(x -> 0, empty_array_uint8());
SELECT array_exists(x -> 0, [1, 2, 3]);
SELECT array_exists(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> 1, empty_array_uint8());
SELECT array_exists(x -> 1, [1, 2, 3]);
SELECT array_exists(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---first---';
SELECT array_first(x -> 0, empty_array_uint8());
SELECT array_first(x -> 0, [1, 2, 3]);
SELECT array_first(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_first(x -> 1, empty_array_uint8());
SELECT array_first(x -> 1, [1, 2, 3]);
SELECT array_first(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---first index---';
SELECT array_first_index(x -> 0, empty_array_uint8());
SELECT array_first_index(x -> 0, [1, 2, 3]);
SELECT array_first_index(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> 1, empty_array_uint8());
SELECT array_first_index(x -> 1, [1, 2, 3]);
SELECT array_first_index(x -> 1, range(number)) FROM system.numbers LIMIT 10;
SELECT '---cumsum---';
SELECT array_cum_sum(x -> 0, empty_array_uint8());
SELECT array_cum_sum(x -> 0, [1, 2, 3]);
SELECT array_cum_sum(x -> 0, range(number)) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> 10, empty_array_uint8());
SELECT array_cum_sum(x -> 10, [1, 2, 3]);
SELECT array_cum_sum(x -> 10, range(number)) FROM system.numbers LIMIT 10;

SELECT '---map--';
SELECT array_map(x -> materialize(123), empty_array_uint8());
SELECT array_map(x -> materialize(123), [1, 2, 3]);
SELECT array_map(x -> materialize(123), range(number)) FROM system.numbers LIMIT 10;
SELECT '---filter--';
SELECT array_filter(x -> materialize(0), empty_array_uint8());
SELECT array_filter(x -> materialize(0), [1, 2, 3]);
SELECT array_filter(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> materialize(1), empty_array_uint8());
SELECT array_filter(x -> materialize(1), [1, 2, 3]);
SELECT array_filter(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '---count---';
SELECT array_count(x -> materialize(0), empty_array_uint8());
SELECT array_count(x -> materialize(0), [1, 2, 3]);
SELECT array_count(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_count(x -> materialize(1), empty_array_uint8());
SELECT array_count(x -> materialize(1), [1, 2, 3]);
SELECT array_count(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '---sum---';
SELECT array_sum(x -> materialize(0), empty_array_uint8());
SELECT array_sum(x -> materialize(0), [1, 2, 3]);
SELECT array_sum(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> materialize(10), empty_array_uint8());
SELECT array_sum(x -> materialize(10), [1, 2, 3]);
SELECT array_sum(x -> materialize(10), range(number)) FROM system.numbers LIMIT 10;
SELECT '---all---';
SELECT array_all(x -> materialize(0), empty_array_uint8());
SELECT array_all(x -> materialize(0), [1, 2, 3]);
SELECT array_all(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_all(x -> materialize(1), empty_array_uint8());
SELECT array_all(x -> materialize(1), [1, 2, 3]);
SELECT array_all(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '---exists---';
SELECT array_exists(x -> materialize(0), empty_array_uint8());
SELECT array_exists(x -> materialize(0), [1, 2, 3]);
SELECT array_exists(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> materialize(1), empty_array_uint8());
SELECT array_exists(x -> materialize(1), [1, 2, 3]);
SELECT array_exists(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '---first---';
SELECT array_first(x -> materialize(0), empty_array_uint8());
SELECT array_first(x -> materialize(0), [1, 2, 3]);
SELECT array_first(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_first(x -> materialize(1), empty_array_uint8());
SELECT array_first(x -> materialize(1), [1, 2, 3]);
SELECT array_first(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '---first index---';
SELECT array_first_index(x -> materialize(0), empty_array_uint8());
SELECT array_first_index(x -> materialize(0), [1, 2, 3]);
SELECT array_first_index(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> materialize(1), empty_array_uint8());
SELECT array_first_index(x -> materialize(1), [1, 2, 3]);
SELECT array_first_index(x -> materialize(1), range(number)) FROM system.numbers LIMIT 10;
SELECT '--cumsum---';
SELECT array_cum_sum(x -> materialize(0), empty_array_uint8());
SELECT array_cum_sum(x -> materialize(0), [1, 2, 3]);
SELECT array_cum_sum(x -> materialize(0), range(number)) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> materialize(10), empty_array_uint8());
SELECT array_cum_sum(x -> materialize(10), [1, 2, 3]);
SELECT array_cum_sum(x -> materialize(10), range(number)) FROM system.numbers LIMIT 10;

SELECT '---map--';
SELECT array_map(x -> 123, empty_array_string());
SELECT array_map(x -> 123, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_map(x -> 123, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---filter--';
SELECT array_filter(x -> 0, empty_array_string());
SELECT array_filter(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_filter(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> 1, empty_array_string());
SELECT array_filter(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_filter(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---count---';
SELECT array_count(x -> 0, empty_array_string());
SELECT array_count(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_count(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_count(x -> 1, empty_array_string());
SELECT array_count(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_count(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---sum---';
SELECT array_sum(x -> 0, empty_array_string());
SELECT array_sum(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_sum(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> 10, empty_array_string());
SELECT array_sum(x -> 10, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_sum(x -> 10, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---all---';
SELECT array_all(x -> 0, empty_array_string());
SELECT array_all(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_all(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_all(x -> 1, empty_array_string());
SELECT array_all(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_all(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---exists---';
SELECT array_exists(x -> 0, empty_array_string());
SELECT array_exists(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_exists(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> 1, empty_array_string());
SELECT array_exists(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_exists(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---first---';
SELECT array_first(x -> 0, empty_array_string());
SELECT array_first(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first(x -> 1, empty_array_string());
SELECT array_first(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---first index---';
SELECT array_first_index(x -> 0, empty_array_string());
SELECT array_first_index(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first_index(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> 1, empty_array_string());
SELECT array_first_index(x -> 1, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first_index(x -> 1, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---cumsum---';
SELECT array_cum_sum(x -> 0, empty_array_string());
SELECT array_cum_sum(x -> 0, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_cum_sum(x -> 0, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> 10, empty_array_string());
SELECT array_cum_sum(x -> 10, array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_cum_sum(x -> 10, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;

SELECT '---map--';
SELECT array_map(x -> materialize(123), empty_array_string());
SELECT array_map(x -> materialize(123), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_map(x -> materialize(123), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---filter--';
SELECT array_filter(x -> materialize(0), empty_array_string());
SELECT array_filter(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_filter(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> materialize(1), empty_array_string());
SELECT array_filter(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_filter(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---count---';
SELECT array_count(x -> materialize(0), empty_array_string());
SELECT array_count(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_count(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_count(x -> materialize(1), empty_array_string());
SELECT array_count(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_count(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---sum---';
SELECT array_sum(x -> materialize(0), empty_array_string());
SELECT array_sum(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_sum(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> materialize(10), empty_array_string());
SELECT array_sum(x -> materialize(10), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_sum(x -> materialize(10), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---all---';
SELECT array_all(x -> materialize(0), empty_array_string());
SELECT array_all(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_all(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_all(x -> materialize(1), empty_array_string());
SELECT array_all(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_all(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---exists---';
SELECT array_exists(x -> materialize(0), empty_array_string());
SELECT array_exists(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_exists(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> materialize(1), empty_array_string());
SELECT array_exists(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_exists(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---first---';
SELECT array_first(x -> materialize(0), empty_array_string());
SELECT array_first(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first(x -> materialize(1), empty_array_string());
SELECT array_first(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---first index---';
SELECT array_first_index(x -> materialize(0), empty_array_string());
SELECT array_first_index(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first_index(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> materialize(1), empty_array_string());
SELECT array_first_index(x -> materialize(1), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_first_index(x -> materialize(1), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT '---cumsum---';
SELECT array_cum_sum(x -> materialize(0), empty_array_string());
SELECT array_cum_sum(x -> materialize(0), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_cum_sum(x -> materialize(0), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> materialize(10), empty_array_string());
SELECT array_cum_sum(x -> materialize(10), array_map(x -> to_string(x), [1, 2, 3]));
SELECT array_cum_sum(x -> materialize(10), array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;

SELECT '--- ---';
SELECT array_map(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_count(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_all(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_first(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> number % 2, range(number)) FROM system.numbers LIMIT 10;
SELECT '--- ---';
SELECT array_map(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_filter(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_count(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_sum(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_all(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_exists(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_first_index(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_cum_sum(x -> number % 2, array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
