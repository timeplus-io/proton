select parse_datetime_best_effort('01/12/2017, 18:31:44');
select parse_datetime32_best_effortUS('01/12/2017, 18:31:44');
select parse_datetime32_best_effort('01/12/2017,18:31:44');
select parse_datetime32_best_effortUS('01/12/2017,18:31:44');
select parse_datetime32_best_effort('01/12/2017 ,   18:31:44');
select parse_datetime32_best_effortUS('01/12/2017    ,18:31:44');
select parse_datetime32_best_effortUS('18:31:44, 31/12/2015');
select parse_datetime32_best_effortUS('18:31:44  , 31/12/2015');
select parse_datetime32_best_effort('18:31:44, 31/12/2015');
select parse_datetime32_best_effort('18:31:44  , 31/12/2015');
select parse_datetime32_best_effort('01/12/2017,'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime32_best_effortUS('18:31:44,,,, 31/12/2015'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime32_best_effortUS('18:31:44, 31/12/2015,'); -- { serverError CANNOT_PARSE_TEXT }
select parse_datetime32_best_effort('01/12/2017, 18:31:44,'); -- { serverError CANNOT_PARSE_TEXT }
select parse_datetime32_best_effort('01/12/2017, ,,,18:31:44'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime32_best_effort('18:31:44  ,,,,, 31/12/2015'); -- { serverError CANNOT_PARSE_DATETIME }
