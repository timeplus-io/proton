select parse_datetime64_best_effort('2.55'); -- { serverError 41 }
select parse_datetime64_best_effort_or_null('2.55');
