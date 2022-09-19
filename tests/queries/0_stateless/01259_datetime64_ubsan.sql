select now64(10); -- { serverError 69 }
select length(to_string(now64(9)));
