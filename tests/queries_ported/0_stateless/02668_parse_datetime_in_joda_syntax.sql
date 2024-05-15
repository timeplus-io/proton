-- { echoOn }
-- empty
select parse_datetime_in_joda_syntax(' ', ' ', 'UTC') = to_datetime('1970-01-01', 'UTC');

-- era
select parse_datetime_in_joda_syntax('AD 1999', 'G YYYY') = to_datetime('1999-01-01');
select parse_datetime_in_joda_syntax('ad 1999', 'G YYYY') = to_datetime('1999-01-01');
select parse_datetime_in_joda_syntax('Ad 1999', 'G YYYY') = to_datetime('1999-01-01');
select parse_datetime_in_joda_syntax('AD 1999', 'G YYYY') = to_datetime('1999-01-01');
select parse_datetime_in_joda_syntax('AD 1999', 'G yyyy') = to_datetime('1999-01-01');
select parse_datetime_in_joda_syntax('AD 1999 2000', 'G YYYY yyyy') = to_datetime('2000-01-01');
select parse_datetime_in_joda_syntax('AD 1999 2000', 'G yyyy YYYY') = to_datetime('2000-01-01');
select parse_datetime_in_joda_syntax('AD 1999', 'G Y'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('AD 1999', 'G YY'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('AD 1999', 'G YYY'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('BC', 'G'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('AB', 'G'); -- { serverError CANNOT_PARSE_DATETIME }

-- year of era
select parse_datetime_in_joda_syntax('2106', 'YYYY', 'UTC') = to_datetime('2106-01-01', 'UTC');
select parse_datetime_in_joda_syntax('1970', 'YYYY', 'UTC') = to_datetime('1970-01-01', 'UTC');
select parse_datetime_in_joda_syntax('1969', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('2107', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('+1999', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

select parse_datetime_in_joda_syntax('12', 'YY', 'UTC') = to_datetime('2012-01-01', 'UTC');
select parse_datetime_in_joda_syntax('69', 'YY', 'UTC') = to_datetime('2069-01-01', 'UTC');
select parse_datetime_in_joda_syntax('70', 'YY', 'UTC') = to_datetime('1970-01-01', 'UTC');
select parse_datetime_in_joda_syntax('99', 'YY', 'UTC') = to_datetime('1999-01-01', 'UTC');
select parse_datetime_in_joda_syntax('01', 'YY', 'UTC') = to_datetime('2001-01-01', 'UTC');
select parse_datetime_in_joda_syntax('1', 'YY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

select parse_datetime_in_joda_syntax('99 98 97', 'YY YY YY', 'UTC') = to_datetime('1997-01-01', 'UTC');

-- year
select parse_datetime_in_joda_syntax('12', 'yy', 'UTC') = to_datetime('2012-01-01', 'UTC');
select parse_datetime_in_joda_syntax('69', 'yy', 'UTC') = to_datetime('2069-01-01', 'UTC');
select parse_datetime_in_joda_syntax('70', 'yy', 'UTC') = to_datetime('1970-01-01', 'UTC');
select parse_datetime_in_joda_syntax('99', 'yy', 'UTC') = to_datetime('1999-01-01', 'UTC');
select parse_datetime_in_joda_syntax('+99', 'yy', 'UTC') = to_datetime('1999-01-01', 'UTC');
select parse_datetime_in_joda_syntax('+99 02', 'yy MM', 'UTC') = to_datetime('1999-02-01', 'UTC');
select parse_datetime_in_joda_syntax('10 +10', 'MM yy', 'UTC') = to_datetime('2010-10-01', 'UTC');
select parse_datetime_in_joda_syntax('10+2001', 'MMyyyy', 'UTC') = to_datetime('2001-10-01', 'UTC');
select parse_datetime_in_joda_syntax('+200110', 'yyyyMM', 'UTC') = to_datetime('2001-10-01', 'UTC');
select parse_datetime_in_joda_syntax('1970', 'yyyy', 'UTC') = to_datetime('1970-01-01', 'UTC');
select parse_datetime_in_joda_syntax('2106', 'yyyy', 'UTC') = to_datetime('2106-01-01', 'UTC');
select parse_datetime_in_joda_syntax('1969', 'yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('2107', 'yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- week year
select parse_datetime_in_joda_syntax('2106', 'xxxx', 'UTC') = to_datetime('2106-01-04', 'UTC');
select parse_datetime_in_joda_syntax('1971', 'xxxx', 'UTC') = to_datetime('1971-01-04', 'UTC');
select parse_datetime_in_joda_syntax('2025', 'xxxx', 'UTC') = to_datetime('2024-12-30', 'UTC');
select parse_datetime_in_joda_syntax('12', 'xx', 'UTC') = to_datetime('2012-01-02', 'UTC');
select parse_datetime_in_joda_syntax('69', 'xx', 'UTC') = to_datetime('2068-12-31', 'UTC');
select parse_datetime_in_joda_syntax('99', 'xx', 'UTC') = to_datetime('1999-01-04', 'UTC');
select parse_datetime_in_joda_syntax('01', 'xx', 'UTC') = to_datetime('2001-01-01', 'UTC');
select parse_datetime_in_joda_syntax('+10', 'xx', 'UTC') = to_datetime('2010-01-04', 'UTC');
select parse_datetime_in_joda_syntax('+99 01', 'xx ww', 'UTC') = to_datetime('1999-01-04', 'UTC');
select parse_datetime_in_joda_syntax('+99 02', 'xx ww', 'UTC') = to_datetime('1999-01-11', 'UTC');
select parse_datetime_in_joda_syntax('10 +10', 'ww xx', 'UTC') = to_datetime('2010-03-08', 'UTC');
select parse_datetime_in_joda_syntax('2+10', 'wwxx', 'UTC') = to_datetime('2010-01-11', 'UTC');
select parse_datetime_in_joda_syntax('+102', 'xxM', 'UTC') = to_datetime('2010-02-01', 'UTC');
select parse_datetime_in_joda_syntax('+20102', 'xxxxM', 'UTC') = to_datetime('2010-02-01', 'UTC');
select parse_datetime_in_joda_syntax('1970', 'xxxx', 'UTC'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
select parse_datetime_in_joda_syntax('1969', 'xxxx', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('2107', 'xxxx', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- century of era
select parse_datetime_in_joda_syntax('20', 'CC', 'UTC') = to_datetime('2000-01-01', 'UTC');
select parse_datetime_in_joda_syntax('21', 'CC', 'UTC') = to_datetime('2100-01-01', 'UTC');
select parse_datetime_in_joda_syntax('19', 'CC', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('22', 'CC', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- month
select parse_datetime_in_joda_syntax('1', 'M', 'UTC') = to_datetime('2000-01-01', 'UTC');
select parse_datetime_in_joda_syntax(' 7', ' MM', 'UTC') = to_datetime('2000-07-01', 'UTC');
select parse_datetime_in_joda_syntax('11', 'M', 'UTC') = to_datetime('2000-11-01', 'UTC');
select parse_datetime_in_joda_syntax('10-', 'M-', 'UTC') = to_datetime('2000-10-01', 'UTC');
select parse_datetime_in_joda_syntax('-12-', '-M-', 'UTC') = to_datetime('2000-12-01', 'UTC');
select parse_datetime_in_joda_syntax('0', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('13', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('12345', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
--- Ensure MMM and MMMM specifiers consume both short- and long-form month names
select parse_datetime_in_joda_syntax('Aug', 'MMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
select parse_datetime_in_joda_syntax('AuG', 'MMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
select parse_datetime_in_joda_syntax('august', 'MMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
select parse_datetime_in_joda_syntax('Aug', 'MMMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
select parse_datetime_in_joda_syntax('AuG', 'MMMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
select parse_datetime_in_joda_syntax('august', 'MMMM', 'UTC') = to_datetime('2000-08-01', 'UTC');
--- invalid month names
select parse_datetime_in_joda_syntax('Decembr', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('Decembr', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('Decemberary', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('Decemberary', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('asdf', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('asdf', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- day of month
select parse_datetime_in_joda_syntax('1', 'd', 'UTC') = to_datetime('2000-01-01', 'UTC');
select parse_datetime_in_joda_syntax('7 ', 'dd ', 'UTC') = to_datetime('2000-01-07', 'UTC');
select parse_datetime_in_joda_syntax('/11', '/dd', 'UTC') = to_datetime('2000-01-11', 'UTC');
select parse_datetime_in_joda_syntax('/31/', '/d/', 'UTC') = to_datetime('2000-01-31', 'UTC');
select parse_datetime_in_joda_syntax('0', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('32', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('12345', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('02-31', 'M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('04-31', 'M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
-- The last one is chosen if multiple day of months are supplied.
select parse_datetime_in_joda_syntax('2 31 1', 'M d M', 'UTC') = to_datetime('2000-01-31', 'UTC');
select parse_datetime_in_joda_syntax('1 31 20 2', 'M d d M', 'UTC') = to_datetime('2000-02-20', 'UTC');
select parse_datetime_in_joda_syntax('2 31 20 4', 'M d d M', 'UTC') = to_datetime('2000-04-20', 'UTC');
--- Leap year
select parse_datetime_in_joda_syntax('2020-02-29', 'YYYY-M-d', 'UTC') = to_datetime('2020-02-29', 'UTC');
select parse_datetime_in_joda_syntax('2001-02-29', 'YYYY-M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- day of year
select parse_datetime_in_joda_syntax('1', 'D', 'UTC') = to_datetime('2000-01-01', 'UTC');
select parse_datetime_in_joda_syntax('7 ', 'DD ', 'UTC') = to_datetime('2000-01-07', 'UTC');
select parse_datetime_in_joda_syntax('/11', '/DD', 'UTC') = to_datetime('2000-01-11', 'UTC');
select parse_datetime_in_joda_syntax('/31/', '/DDD/', 'UTC') = to_datetime('2000-01-31', 'UTC');
select parse_datetime_in_joda_syntax('32', 'D', 'UTC') = to_datetime('2000-02-01', 'UTC');
select parse_datetime_in_joda_syntax('60', 'D', 'UTC') = to_datetime('2000-02-29', 'UTC');
select parse_datetime_in_joda_syntax('365', 'D', 'UTC') = to_datetime('2000-12-30', 'UTC');
select parse_datetime_in_joda_syntax('366', 'D', 'UTC') = to_datetime('2000-12-31', 'UTC');
select parse_datetime_in_joda_syntax('1999 1', 'yyyy D', 'UTC') = to_datetime('1999-01-01', 'UTC');
select parse_datetime_in_joda_syntax('1999 7 ', 'yyyy DD ', 'UTC') = to_datetime('1999-01-07', 'UTC');
select parse_datetime_in_joda_syntax('1999 /11', 'yyyy /DD', 'UTC') = to_datetime('1999-01-11', 'UTC');
select parse_datetime_in_joda_syntax('1999 /31/', 'yyyy /DD/', 'UTC') = to_datetime('1999-01-31', 'UTC');
select parse_datetime_in_joda_syntax('1999 32', 'yyyy D', 'UTC') = to_datetime('1999-02-01', 'UTC');
select parse_datetime_in_joda_syntax('1999 60', 'yyyy D', 'UTC') = to_datetime('1999-03-01', 'UTC');
select parse_datetime_in_joda_syntax('1999 365', 'yyyy D', 'UTC') = to_datetime('1999-12-31', 'UTC');
select parse_datetime_in_joda_syntax('1999 366', 'yyyy D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
--- Ensure all days of year are checked against final selected year
select parse_datetime_in_joda_syntax('2001 366 2000', 'yyyy D yyyy', 'UTC') = to_datetime('2000-12-31', 'UTC');
select parse_datetime_in_joda_syntax('2000 366 2001', 'yyyy D yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('0', 'D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('367', 'D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- hour of day
select parse_datetime_in_joda_syntax('7', 'H', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('23', 'HH', 'UTC') = to_datetime('1970-01-01 23:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0', 'HHH', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('10', 'HHHHHHHH', 'UTC') = to_datetime('1970-01-01 10:00:00', 'UTC');
--- invalid hour od day
select parse_datetime_in_joda_syntax('24', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('-1', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- clock hour of day
select parse_datetime_in_joda_syntax('7', 'k', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('24', 'kk', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1', 'kkk', 'UTC') = to_datetime('1970-01-01 01:00:00', 'UTC');
select parse_datetime_in_joda_syntax('10', 'kkkkkkkk', 'UTC') = to_datetime('1970-01-01 10:00:00', 'UTC');
-- invalid clock hour of day
select parse_datetime_in_joda_syntax('25', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('0', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- hour of half day
select parse_datetime_in_joda_syntax('7', 'K', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('11', 'KK', 'UTC') = to_datetime('1970-01-01 11:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0', 'KKK', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('10', 'KKKKKKKK', 'UTC') = to_datetime('1970-01-01 10:00:00', 'UTC');
-- invalid hour of half day
select parse_datetime_in_joda_syntax('12', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('-1', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- clock hour of half day
select parse_datetime_in_joda_syntax('7', 'h', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('12', 'hh', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1', 'hhh', 'UTC') = to_datetime('1970-01-01 01:00:00', 'UTC');
select parse_datetime_in_joda_syntax('10', 'hhhhhhhh', 'UTC') = to_datetime('1970-01-01 10:00:00', 'UTC');
-- invalid clock hour of half day
select parse_datetime_in_joda_syntax('13', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('0', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- half of day
--- Half of day has no effect if hour or clockhour of day is provided hour of day tests
select parse_datetime_in_joda_syntax('7 PM', 'H a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 AM', 'H a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 pm', 'H a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 am', 'H a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0 PM', 'H a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0 AM', 'H a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0 pm', 'H a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0 am', 'H a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 PM', 'k a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 AM', 'k a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 pm', 'k a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('7 am', 'k a', 'UTC') = to_datetime('1970-01-01 07:00:00', 'UTC');
select parse_datetime_in_joda_syntax('24 PM', 'k a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('24 AM', 'k a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('24 pm', 'k a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('24 am', 'k a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
-- Half of day has effect if hour or clockhour of halfday is provided
select parse_datetime_in_joda_syntax('0 PM', 'K a', 'UTC') = to_datetime('1970-01-01 12:00:00', 'UTC');
select parse_datetime_in_joda_syntax('0 AM', 'K a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('6 PM', 'K a', 'UTC') = to_datetime('1970-01-01 18:00:00', 'UTC');
select parse_datetime_in_joda_syntax('6 AM', 'K a', 'UTC') = to_datetime('1970-01-01 06:00:00', 'UTC');
select parse_datetime_in_joda_syntax('11 PM', 'K a', 'UTC') = to_datetime('1970-01-01 23:00:00', 'UTC');
select parse_datetime_in_joda_syntax('11 AM', 'K a', 'UTC') = to_datetime('1970-01-01 11:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1 PM', 'h a', 'UTC') = to_datetime('1970-01-01 13:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1 AM', 'h a', 'UTC') = to_datetime('1970-01-01 01:00:00', 'UTC');
select parse_datetime_in_joda_syntax('6 PM', 'h a', 'UTC') = to_datetime('1970-01-01 18:00:00', 'UTC');
select parse_datetime_in_joda_syntax('6 AM', 'h a', 'UTC') = to_datetime('1970-01-01 06:00:00', 'UTC');
select parse_datetime_in_joda_syntax('12 PM', 'h a', 'UTC') = to_datetime('1970-01-01 12:00:00', 'UTC');
select parse_datetime_in_joda_syntax('12 AM', 'h a', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
-- time gives precendent to most recent time specifier
select parse_datetime_in_joda_syntax('0 1 AM', 'H h a', 'UTC') = to_datetime('1970-01-01 01:00:00', 'UTC');
select parse_datetime_in_joda_syntax('12 1 PM', 'H h a', 'UTC') = to_datetime('1970-01-01 13:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1 AM 0', 'h a H', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('1 AM 12', 'h a H', 'UTC') = to_datetime('1970-01-01 12:00:00', 'UTC');

-- minute
select parse_datetime_in_joda_syntax('8', 'm', 'UTC') = to_datetime('1970-01-01 00:08:00', 'UTC');
select parse_datetime_in_joda_syntax('59', 'mm', 'UTC') = to_datetime('1970-01-01 00:59:00', 'UTC');
select parse_datetime_in_joda_syntax('0/', 'mmm/', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('60', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('-1', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- second
select parse_datetime_in_joda_syntax('9', 's', 'UTC') = to_datetime('1970-01-01 00:00:09', 'UTC');
select parse_datetime_in_joda_syntax('58', 'ss', 'UTC') = to_datetime('1970-01-01 00:00:58', 'UTC');
select parse_datetime_in_joda_syntax('0/', 's/', 'UTC') = to_datetime('1970-01-01 00:00:00', 'UTC');
select parse_datetime_in_joda_syntax('60', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('-1', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parse_datetime_in_joda_syntax('123456789', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- { echoOff }
