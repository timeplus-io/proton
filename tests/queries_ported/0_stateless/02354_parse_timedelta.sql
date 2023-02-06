SELECT parse_time_delta('1 min 35 sec');
SELECT parse_time_delta('0m;11.23s.');
SELECT parse_time_delta('11hr 25min 3.1s');
SELECT parse_time_delta('0.00123 seconds');
SELECT parse_time_delta('1yr2mo');
SELECT parse_time_delta('11s+22min');
SELECT parse_time_delta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ; 33 seconds');
SELECT parse_time_delta('1s1ms1us1ns');
SELECT parse_time_delta('1s1ms1μs1ns');
SELECT parse_time_delta('1s - 1ms : 1μs ; 1ns');
SELECT parse_time_delta('1.11s1.11ms1.11us1.11ns');

-- invalid expressions
SELECT parse_time_delta(); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT parse_time_delta('1yr', 1); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT parse_time_delta(1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parse_time_delta(' '); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('-1yr'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1yr-'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('yr2mo'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1.yr2mo'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1-yr'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1 1yr'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1yyr'); -- {serverError BAD_ARGUMENTS}
SELECT parse_time_delta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ;. 33 seconds'); -- {serverError BAD_ARGUMENTS}
