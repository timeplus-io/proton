SET allow_experimental_analyzer = 1;

SELECT array_map(x -> x * sum(x), range(10)); -- { serverError 10 }