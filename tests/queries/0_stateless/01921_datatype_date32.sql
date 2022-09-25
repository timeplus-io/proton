SET query_mode = 'table';
drop stream if exists t1;
create stream t1(x1 date32) engine Memory;

insert into t1 values ('1925-01-01'),('1924-01-01'),('2282-12-31'),('2283-12-31'),('2021-06-22');

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
select '-------toDayOfYear---------';
select toDayOfYear(x1) from t1;
select '-------to_hour---------';
select to_hour(x1) from t1; -- { serverError 43 }
select '-------to_minute---------';
select to_minute(x1) from t1; -- { serverError 43 }
select '-------to_second---------';
select to_second(x1) from t1; -- { serverError 43 }
select '-------to_start_of_day---------';
select to_start_of_day(x1, 'Europe/Moscow') from t1;
select '-------to_monday---------';
select to_monday(x1) from t1;
select '-------toISOWeek---------';
select toISOWeek(x1) from t1;
select '-------toISOYear---------';
select toISOYear(x1) from t1;
select '-------toWeek---------';
select toWeek(x1) from t1;
select '-------toYearWeek---------';
select toYearWeek(x1) from t1;
select '-------toStartOfWeek---------';
select toStartOfWeek(x1) from t1;
select '-------toStartOfMonth---------';
select to_start_of_month(x1) from t1;
select '-------to_start_of_quarter---------';
select to_start_of_quarter(x1) from t1;
select '-------to_start_of_year---------';
select to_start_of_year(x1) from t1;
select '-------toStartOfSecond---------';
select toStartOfSecond(x1) from t1; -- { serverError 43 }
select '-------to_start_of_minute---------';
select to_start_of_minute(x1) from t1; -- { serverError 43 }
select '-------to_start_of_five_minute---------';
select to_start_of_five_minute(x1) from t1; -- { serverError 43 }
select '-------to_start_of_ten_minute---------';
select to_start_of_ten_minute(x1) from t1; -- { serverError 43 }
select '-------to_start_of_fifteen_minute---------';
select to_start_of_fifteen_minutes(x1) from t1; -- { serverError 43 }
select '-------to_start_of_hour---------';
select to_start_of_hour(x1) from t1; -- { serverError 43 }
select '-------toStartOfISOYear---------';
select toStartOfISOYear(x1) from t1;
select '-------to_relative_year_num---------';
select to_relative_year_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_quarter_num---------';
select to_relative_quarter_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_month_num---------';
select to_relative_month_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_week_num---------';
select to_relative_week_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_day_num---------';
select to_relative_day_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_hour_num---------';
select to_relative_hour_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_minute_num---------';
select to_relative_minute_num(x1, 'Europe/Moscow') from t1;
select '-------to_relative_second_num---------';
select to_relative_second_num(x1, 'Europe/Moscow') from t1;
select '-------to_time---------';
select to_time(x1) from t1; -- { serverError 43 }
select '-------to_YYYYMM---------';
select to_YYYYMM(x1) from t1;
select '-------to_YYYYMMDD---------';
select to_YYYYMMDD(x1) from t1;
select '-------to_YYYYMMDDhhmmss---------';
select to_YYYYMMDDhhmmss(x1) from t1;
select '-------addSeconds---------';
select addSeconds(x1, 3600) from t1;
select '-------addMinutes---------';
select addMinutes(x1, 60) from t1;
select '-------addHours---------';
select addHours(x1, 1) from t1;
select '-------addDays---------';
select addDays(x1, 7) from t1;
select '-------addWeeks---------';
select addWeeks(x1, 1) from t1;
select '-------add_months---------';
select add_months(x1, 1) from t1;
select '-------addQuarters---------';
select addQuarters(x1, 1) from t1;
select '-------addYears---------';
select addYears(x1, 1) from t1;
select '-------subtractSeconds---------';
select subtractSeconds(x1, 3600) from t1;
select '-------subtractMinutes---------';
select subtractMinutes(x1, 60) from t1;
select '-------subtractHours---------';
select subtractHours(x1, 1) from t1;
select '-------subtractDays---------';
select subtractDays(x1, 7) from t1;
select '-------subtractWeeks---------';
select subtractWeeks(x1, 1) from t1;
select '-------subtractMonths---------';
select subtractMonths(x1, 1) from t1;
select '-------subtractQuarters---------';
select subtractQuarters(x1, 1) from t1;
select '-------subtractYears---------';
select subtractYears(x1, 1) from t1;
select '-------toDate32---------';
select toDate32('1925-01-01'), toDate32(to_date('2000-01-01'));
select toDate32OrZero('1924-01-01'), toDate32OrNull('1924-01-01');
select toDate32OrZero(''), toDate32OrNull('');
select (select toDate32OrZero(''));
select (select toDate32OrNull(''));

