SELECT format_datetime(to_date('2010-01-01'), '%G'); -- Friday (first day of the year) attributed to week 53 of the previous year (2009)
SELECT format_datetime(to_date('2010-01-01'), '%g');
SELECT format_datetime(to_date('2010-01-03'), '%G'); -- Sunday, last day attributed to week 53 of the previous year (2009)
SELECT format_datetime(to_date('2010-01-03'), '%g');
SELECT format_datetime(to_date('2010-01-04'), '%G'); -- Monday, first day in the year attributed to week 01 of the current year (2010)
SELECT format_datetime(to_date('2010-01-04'), '%g');
SELECT format_datetime(to_date('2018-12-31'), '%G'); -- Monday (last day of the year) attributed to 01 week of next year (2019)
SELECT format_datetime(to_date('2018-12-31'), '%g');
SELECT format_datetime(to_date('2019-01-01'), '%G'); -- Tuesday (first day of the year) attributed to 01 week of this year (2019)
SELECT format_datetime(to_date('2019-01-01'), '%g');
