SELECT finalize_aggregation((SELECT sum_state(number) FROM numbers(10)) + (SELECT sum_state(number) FROM numbers(10)));
SELECT finalize_aggregation((SELECT sum_state(number) FROM numbers(10)) + materialize((SELECT sum_state(number) FROM numbers(10))));
SELECT finalize_aggregation(materialize((SELECT sum_state(number) FROM numbers(10))) + (SELECT sum_state(number) FROM numbers(10)));
SELECT finalize_aggregation(materialize((SELECT sum_state(number) FROM numbers(10))) + materialize((SELECT sum_state(number) FROM numbers(10))));
SELECT finalize_aggregation(materialize((SELECT sum_state(number) FROM numbers(10)) + (SELECT sum_state(number) FROM numbers(10))));
SELECT materialize(finalize_aggregation((SELECT sum_state(number) FROM numbers(10)) + (SELECT sum_state(number) FROM numbers(10))));
