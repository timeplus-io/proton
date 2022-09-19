SELECT ignore(addDays(to_datetime(0), -1));
SELECT ignore(subtractDays(to_datetime(0), 1));

SELECT ignore(addDays(to_date(0), -1));
SELECT ignore(subtractDays(to_date(0), 1));

SET send_logs_level = 'fatal';

SELECT ignore(addDays((CAST((96.338) AS DateTime)), -3));
SELECT ignore(subtractDays((CAST((-5263074.47) AS DateTime)), -737895));
SELECT quantileDeterministic([], identity(( SELECT subtractDays((CAST((566450.398706) AS DateTime)), 54) ) )), '\0', []; -- { serverError 43 }
SELECT sequenceCount((CAST((( SELECT NULL ) AS rg, ( SELECT ( SELECT [], '<e', caseWithExpr([NULL], -588755.149, []), retention(addWeeks((CAST((-7644612.39732) AS DateTime)), -23578040.02833), (CAST(([]) AS DateTime)), (CAST(([010977.08]) AS string))), emptyArrayToSingle('') ) , '\0', to_uint64([], 't3hw@'), '\0', to_start_of_quarter(-4230.1872, []) ) ) AS date))); -- { serverError 43 }
