SELECT uniq_exact(x) FROM (SELECT uniq_state(number) AS x FROM numbers(100));
SELECT uniq_exact(x) FROM (SELECT uniq_state(number) AS x FROM numbers(1000));
SELECT hex(to_string(uniqExactState(x))) FROM (SELECT uniq_state(number) AS x FROM numbers(1000));
SELECT hex(to_string(uniqExactState(x))) FROM (SELECT quantileState(number) AS x FROM numbers(1000));
SELECT to_type_name(uniqExactState(x)) FROM (SELECT quantileState(number) AS x FROM numbers(1000));
SELECT to_type_name(initializeAggregation('uniq_exact', 0));
SELECT to_type_name(initializeAggregation('uniqExactState', 0));
SELECT to_type_name(initializeAggregation('uniqExactState', initializeAggregation('quantileState', 0)));
SELECT hex(to_string(initializeAggregation('quantileState', 0)));
SELECT to_type_name(initializeAggregation('sumState', initializeAggregation('quantileState', 0))); -- { serverError 43 }
SELECT to_type_name(initializeAggregation('anyState', initializeAggregation('quantileState', 0)));
SELECT to_type_name(initializeAggregation('anyState', initializeAggregation('uniq_state', 0)));
SELECT hex(to_string(initializeAggregation('uniq_state', initializeAggregation('uniq_state', 0))));
SELECT hex(to_string(initializeAggregation('uniq_state', initializeAggregation('quantileState', 0))));
SELECT hex(to_string(initializeAggregation('anyLastState', initializeAggregation('uniq_state', 0))));
SELECT hex(to_string(initializeAggregation('anyState', initializeAggregation('uniq_state', 0))));
SELECT hex(to_string(initializeAggregation('maxState', initializeAggregation('uniq_state', 0)))); -- { serverError 43 }
SELECT hex(to_string(initializeAggregation('uniqExactState', initializeAggregation('uniq_state', 0))));
SELECT finalize_aggregation(initializeAggregation('uniqExactState', initializeAggregation('uniq_state', 0)));
SELECT to_type_name(quantileState(x)) FROM (SELECT uniq_state(number) AS x FROM numbers(1000)); -- { serverError 43 }
SELECT hex(to_string(quantileState(x))) FROM (SELECT uniq_state(number) AS x FROM numbers(1000)); -- { serverError 43 }
SELECT hex(to_string(anyState(x))), hex(to_string(any(x))) FROM (SELECT uniq_state(number) AS x FROM numbers(1000)) FORMAT Vertical;
