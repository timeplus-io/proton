SELECT hex(to_string(uniqStateForEach([1, NULL])));
SELECT hex(to_string(uniqStateForEachState([1, NULL])));
SELECT array_map(x -> hex(to_string(x)), finalize_aggregation(uniqStateForEachState([1, NULL])));
SELECT array_map(x -> finalize_aggregation(x), finalize_aggregation(uniqStateForEachState([1, NULL])));

SELECT hex(to_string(uniqStateForEach([1, NULL]))) WITH TOTALS;
SELECT hex(to_string(uniqStateForEachState([1, NULL]))) WITH TOTALS;
SELECT array_map(x -> hex(to_string(x)), finalize_aggregation(uniqStateForEachState([1, NULL]))) WITH TOTALS;
SELECT array_map(x -> finalize_aggregation(x), finalize_aggregation(uniqStateForEachState([1, NULL]))) WITH TOTALS;
