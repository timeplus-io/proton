SELECT today() AS a
ORDER BY a ASC WITH FILL FROM now() - to_Interval_Month(1) TO now() + to_Interval_Day(1) STEP 82600; -- { serverError 475 }
