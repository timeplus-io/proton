SELECT
    round(sigmoid(x), 5), round(sigmoid(to_float32(x)), 5), round(sigmoid(to_float64(x)), 5),
    round(tanh(x), 5), round(TANH(to_float32(x)), 5), round(TANh(to_float64(x)), 5)
FROM (SELECT array_join([-1, 0, 1]) AS x);
