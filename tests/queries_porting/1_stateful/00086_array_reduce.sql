SELECT array_filter(x -> x != 1, array_map((a, b) -> a = b, GeneralInterests, arrayReduce('groupArray', GeneralInterests))) AS res from table(test.hits) WHERE length(res) != 0;
