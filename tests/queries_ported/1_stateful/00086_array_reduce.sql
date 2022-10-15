SELECT array_filter(x -> x != 1, array_map((a, b) -> a = b, Generalinterests, array_reduce('group_array', Generalinterests))) AS res from table(test.hits) WHERE length(res) != 0;
