SELECT from_modified_julian_day(9223372036854775807 :: int64); -- { serverError 490 }
