SELECT array_filter(x -> x != 1, array_map((a, b) -> a = b, GeneralInterests, array_reduce('group_array', GeneralInterests))) AS res FROM test.hits WHERE length(res) != 0;
