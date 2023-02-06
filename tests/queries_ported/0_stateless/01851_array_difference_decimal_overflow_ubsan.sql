SELECT array_difference([to_decimal32(100.0000991821289, 0), -2147483647]) AS x; --{serverError 407}
