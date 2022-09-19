SELECT finalize_aggregation((SELECT sumState(number) FROM numbers(10)) * 10);
SELECT finalize_aggregation(materialize((SELECT sumState(number) FROM numbers(10))) * 10);
SELECT finalize_aggregation(materialize((SELECT sumState(number) FROM numbers(10)) * 10));
SELECT materialize(finalize_aggregation((SELECT sumState(number) FROM numbers(10)) * 10));
