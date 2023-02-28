
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon), to_day_of_week(date_sun);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 0), to_day_of_week(date_sun, 0);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 1), to_day_of_week(date_sun, 1);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 2), to_day_of_week(date_sun, 2);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 3), to_day_of_week(date_sun, 3);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 4), to_day_of_week(date_sun, 4);
with to_date('2023-01-09') as date_mon, date_mon - 1 as date_sun select to_day_of_week(date_mon, 5), to_day_of_week(date_sun, 5);

select to_day_of_week(today(), -1); -- { serverError 43 }
