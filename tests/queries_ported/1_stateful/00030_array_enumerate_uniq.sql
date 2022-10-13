SELECT max(array_join(array_enumerate_uniq(array_map(x -> int_div(x, 10), URLCategories)))) from table(test.hits)
