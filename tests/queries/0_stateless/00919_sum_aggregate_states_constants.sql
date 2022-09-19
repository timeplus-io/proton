SELECT finalize_aggregation((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10)));
SELECT finalize_aggregation((SELECT sumState(number) FROM numbers(10)) + materialize((SELECT sumState(number) FROM numbers(10))));
SELECT finalize_aggregation(materialize((SELECT sumState(number) FROM numbers(10))) + (SELECT sumState(number) FROM numbers(10)));
SELECT finalize_aggregation(materialize((SELECT sumState(number) FROM numbers(10))) + materialize((SELECT sumState(number) FROM numbers(10))));
SELECT finalize_aggregation(materialize((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10))));
SELECT materialize(finalize_aggregation((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10))));
