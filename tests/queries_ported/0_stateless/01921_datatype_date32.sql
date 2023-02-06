drop stream if exists t1;
create stream t1(x1 Date32) engine Memory;

insert into t1 values ('1900-01-01'),('1899-01-01'),('2299-12-15'),('2300-12-31'),('2021-06-22');

select x1 from t1;
select '-------to_year---------';
select to_year(x1) from t1;
select '-------to_month---------';
select to_month(x1) from t1;
select '-------to_quarter---------';
select to_quarter(x1) from t1;
select '-------to_day_of_month---------';
select to_day_of_month(x1) from t1;
select '-------to_day_of_week---------';
select to_day_of_week(x1) from t1;
select '-------to_day_of_year---------';
select to_day_of_year(x1) from t1;
select '-------to_hour---------';
select to_hour(x1) from t1; -- { serverError 43 }
select '-------to_minute---------';
select to_minute(x1) from t1; -- { serverError 43 }
select '-------to_second---------';
select to_second(x1) from t1; -- { serverError 43 }
select '-------to_start_of_day---------';
select to_start_of_day(x1, 'Asia/Istanbul') from t1;
select '-------to_monday---------';
select to_monday(x1) from t1;
select '-------to_iso_week---------';
select to_iso_week(x1) from t1;
select '-------to_iso_year---------';
select to_iso_year(x1) from t1;
select '-------to_week---------';
select to_week(x1) from t1;
select '-------to_year_week---------';
select to_year_week(x1) from t1;
select '-------to_start_of_week---------';
select to_start_of_week(x1) from t1;
select '-------to_start_of_month---------';
select to_start_of_month(x1) from t1;
select '-------to_start_of_quarter---------';
select to_start_of_quarter(x1) from t1;
select '-------to_start_of_year---------';
select to_start_of_year(x1) from t1;
select '-------to_start_of_second---------';
select to_start_of_second(x1) from t1; -- { serverError 43 }
select '-------to_start_of_minute---------';
select to_start_of_minute(x1) from t1; -- { serverError 43 }
select '-------to_start_of_five_minutes---------';
select to_start_of_five_minutes(x1) from t1; -- { serverError 43 }
select '-------to_start_of_ten_minutes---------';
select to_start_of_ten_minutes(x1) from t1; -- { serverError 43 }
select '-------to_start_of_fifteen_minutes---------';
select to_start_of_fifteen_minutes(x1) from t1; -- { serverError 43 }
select '-------to_start_of_hour---------';
select to_start_of_hour(x1) from t1; -- { serverError 43 }
select '-------to_start_of_iso_year---------';
select to_start_of_iso_year(x1) from t1;
select '-------to_relative_year_num---------';
select to_relative_year_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_quarter_num---------';
select to_relative_quarter_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_month_num---------';
select to_relative_month_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_week_num---------';
select to_relative_week_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_day_num---------';
select to_relative_day_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_hour_num---------';
select to_relative_hour_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_minute_num---------';
select to_relative_minute_num(x1, 'Asia/Istanbul') from t1;
select '-------to_relative_second_num---------';
select to_relative_second_num(x1, 'Asia/Istanbul') from t1;
select '-------to_time---------';
select to_time(x1) from t1; -- { serverError 43 }
select '-------to__yyyymm---------';
select to_YYYYMM(x1) from t1;
select '-------to__yyyymmDD---------';
select to_YYYYMMDD(x1) from t1;
select '-------to__yyyymmDDhhmmss---------';
select to_YYYYMMDDhhmmss(x1) from t1;
select '-------add_seconds---------';
select add_seconds(x1, 3600) from t1;
select '-------add_minutes---------';
select add_minutes(x1, 60) from t1;
select '-------add_hours---------';
select add_hours(x1, 1) from t1;
select '-------add_days---------';
select add_days(x1, 7) from t1;
select '-------add_weeks---------';
select add_weeks(x1, 1) from t1;
select '-------add_months---------';
select add_months(x1, 1) from t1;
select '-------add_quarters---------';
select add_quarters(x1, 1) from t1;
select '-------add_years---------';
select add_years(x1, 1) from t1;
select '-------subtract_seconds---------';
select subtract_seconds(x1, 3600) from t1;
select '-------subtract_minutes---------';
select subtract_minutes(x1, 60) from t1;
select '-------subtract_hours---------';
select subtract_hours(x1, 1) from t1;
select '-------subtract_days---------';
select subtract_days(x1, 7) from t1;
select '-------subtract_weeks---------';
select subtract_weeks(x1, 1) from t1;
select '-------subtract_months---------';
select subtract_months(x1, 1) from t1;
select '-------subtract_quarters---------';
select subtract_quarters(x1, 1) from t1;
select '-------subtract_years---------';
select subtract_years(x1, 1) from t1;
select '-------to_date---------';
select to_date('1900-01-01'), to_date(to_date('2000-01-01'));
select to_date32_or_zero('1899-01-01'), to_date32_or_null('1899-01-01');
select to_date32_or_zero(''), to_date32_or_null('');
select (select to_date32_or_zero(''));
select (select to_date32_or_null(''));
SELECT to_string(T.d) as dateStr
FROM
    (
    SELECT '1900-01-01'::Date32 as d
    UNION ALL SELECT '1969-12-31'::Date32
    UNION ALL SELECT '1970-01-01'::Date32
    UNION ALL SELECT '2149-06-06'::Date32
    UNION ALL SELECT '2149-06-07'::Date32
    UNION ALL SELECT '2299-12-31'::Date32
    ) AS T
ORDER BY T.d
