SELECT any(nullIf(s, '')) FROM (SELECT array_join(['', 'Hello']) AS s);

SET optimize_move_functions_out_of_any = 0;
EXPLAIN SYNTAX select any(nullIf('', ''), 'some text'); -- { serverError 42 }
SET optimize_move_functions_out_of_any = 1;
EXPLAIN SYNTAX select any(nullIf('', ''), 'some text'); -- { serverError 42 }
