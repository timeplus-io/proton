WITH (SELECT stochasticLinearRegressionState(1, 2, 3)) AS model SELECT evalMLMethod(model, to_float64(1), to_float64(1));
