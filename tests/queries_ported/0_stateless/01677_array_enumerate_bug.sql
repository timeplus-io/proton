-- there was a bug - missing check of the total size of keys for the case with hash stream with 128bit key.

SELECT array_enumerate_uniq(array_enumerate_uniq([to_int256(10), to_int256(100), to_int256(2)]), [to_int256(123), to_int256(1023), to_int256(123)]);

SELECT array_enumerate_uniq(
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666]);
