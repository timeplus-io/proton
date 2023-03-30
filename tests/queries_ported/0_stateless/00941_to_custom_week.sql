-- week mode [0,7],  week test case. refer to the mysql test case
SELECT to_week(to_date('1998-01-01')), to_week(to_date('1997-01-01')), to_week(to_date('1998-01-01'), 1),  to_week(to_date('1997-01-01'), 1);
SELECT to_week(to_date('1998-12-31')), to_week(to_date('1997-12-31')), to_week(to_date('1998-12-31'), 1), to_week(to_date('1997-12-31'), 1);
SELECT to_week(to_date('1995-01-01')), to_week(to_date('1995-01-01'), 1);
SELECT to_year_week(to_date('1981-12-31'), 1), to_year_week(to_date('1982-01-01'), 1), to_year_week(to_date('1982-12-31'), 1), to_year_week(to_date('1983-01-01'), 1);
SELECT to_year_week(to_date('1987-01-01'), 1), to_year_week(to_date('1987-01-01'));	
	
SELECT to_week(to_date('2000-01-01'),0) AS w2000, to_week(to_date('2001-01-01'),0) AS w2001, to_week(to_date('2002-01-01'),0) AS w2002,to_week(to_date('2003-01-01'),0) AS w2003, to_week(to_date('2004-01-01'),0) AS w2004, to_week(to_date('2005-01-01'),0) AS w2005, to_week(to_date('2006-01-01'),0) AS w2006;
SELECT to_week(to_date('2000-01-06'),0) AS w2000, to_week(to_date('2001-01-06'),0) AS w2001, to_week(to_date('2002-01-06'),0) AS w2002,to_week(to_date('2003-01-06'),0) AS w2003, to_week(to_date('2004-01-06'),0) AS w2004, to_week(to_date('2005-01-06'),0) AS w2005, to_week(to_date('2006-01-06'),0) AS w2006;
SELECT to_week(to_date('2000-01-01'),1) AS w2000, to_week(to_date('2001-01-01'),1) AS w2001, to_week(to_date('2002-01-01'),1) AS w2002,to_week(to_date('2003-01-01'),1) AS w2003, to_week(to_date('2004-01-01'),1) AS w2004, to_week(to_date('2005-01-01'),1) AS w2005, to_week(to_date('2006-01-01'),1) AS w2006;
SELECT to_week(to_date('2000-01-06'),1) AS w2000, to_week(to_date('2001-01-06'),1) AS w2001, to_week(to_date('2002-01-06'),1) AS w2002,to_week(to_date('2003-01-06'),1) AS w2003, to_week(to_date('2004-01-06'),1) AS w2004, to_week(to_date('2005-01-06'),1) AS w2005, to_week(to_date('2006-01-06'),1) AS w2006;
SELECT to_year_week(to_date('2000-01-01'),0) AS w2000, to_year_week(to_date('2001-01-01'),0) AS w2001, to_year_week(to_date('2002-01-01'),0) AS w2002,to_year_week(to_date('2003-01-01'),0) AS w2003, to_year_week(to_date('2004-01-01'),0) AS w2004, to_year_week(to_date('2005-01-01'),0) AS w2005, to_year_week(to_date('2006-01-01'),0) AS w2006;
SELECT to_year_week(to_date('2000-01-06'),0) AS w2000, to_year_week(to_date('2001-01-06'),0) AS w2001, to_year_week(to_date('2002-01-06'),0) AS w2002,to_year_week(to_date('2003-01-06'),0) AS w2003, to_year_week(to_date('2004-01-06'),0) AS w2004, to_year_week(to_date('2005-01-06'),0) AS w2005, to_year_week(to_date('2006-01-06'),0) AS w2006;
SELECT to_year_week(to_date('2000-01-01'),1) AS w2000, to_year_week(to_date('2001-01-01'),1) AS w2001, to_year_week(to_date('2002-01-01'),1) AS w2002,to_year_week(to_date('2003-01-01'),1) AS w2003, to_year_week(to_date('2004-01-01'),1) AS w2004, to_year_week(to_date('2005-01-01'),1) AS w2005, to_year_week(to_date('2006-01-01'),1) AS w2006;
SELECT to_year_week(to_date('2000-01-06'),1) AS w2000, to_year_week(to_date('2001-01-06'),1) AS w2001, to_year_week(to_date('2002-01-06'),1) AS w2002,to_year_week(to_date('2003-01-06'),1) AS w2003, to_year_week(to_date('2004-01-06'),1) AS w2004, to_year_week(to_date('2005-01-06'),1) AS w2005, to_year_week(to_date('2006-01-06'),1) AS w2006;	
SELECT to_week(to_date('1998-12-31'),2),to_week(to_date('1998-12-31'),3), to_week(to_date('2000-01-01'),2), to_week(to_date('2000-01-01'),3);
SELECT to_week(to_date('2000-12-31'),2),to_week(to_date('2000-12-31'),3);

SELECT to_week(to_date('1998-12-31'),0) AS w0, to_week(to_date('1998-12-31'),1) AS w1, to_week(to_date('1998-12-31'),2) AS w2, to_week(to_date('1998-12-31'),3) AS w3, to_week(to_date('1998-12-31'),4) AS w4, to_week(to_date('1998-12-31'),5) AS w5, to_week(to_date('1998-12-31'),6) AS w6, to_week(to_date('1998-12-31'),7) AS w7;
SELECT to_week(to_date('2000-01-01'),0) AS w0, to_week(to_date('2000-01-01'),1) AS w1, to_week(to_date('2000-01-01'),2) AS w2, to_week(to_date('2000-01-01'),3) AS w3, to_week(to_date('2000-01-01'),4) AS w4, to_week(to_date('2000-01-01'),5) AS w5, to_week(to_date('2000-01-01'),6) AS w6, to_week(to_date('2000-01-01'),7) AS w7;
SELECT to_week(to_date('2000-01-06'),0) AS w0, to_week(to_date('2000-01-06'),1) AS w1, to_week(to_date('2000-01-06'),2) AS w2, to_week(to_date('2000-01-06'),3) AS w3, to_week(to_date('2000-01-06'),4) AS w4, to_week(to_date('2000-01-06'),5) AS w5, to_week(to_date('2000-01-06'),6) AS w6, to_week(to_date('2000-01-06'),7) AS w7;
SELECT to_week(to_date('2000-12-31'),0) AS w0, to_week(to_date('2000-12-31'),1) AS w1, to_week(to_date('2000-12-31'),2) AS w2, to_week(to_date('2000-12-31'),3) AS w3, to_week(to_date('2000-12-31'),4) AS w4, to_week(to_date('2000-12-31'),5) AS w5, to_week(to_date('2000-12-31'),6) AS w6, to_week(to_date('2000-12-31'),7) AS w7;
SELECT to_week(to_date('2001-01-01'),0) AS w0, to_week(to_date('2001-01-01'),1) AS w1, to_week(to_date('2001-01-01'),2) AS w2, to_week(to_date('2001-01-01'),3) AS w3, to_week(to_date('2001-01-01'),4) AS w4, to_week(to_date('2001-01-01'),5) AS w5, to_week(to_date('2001-01-01'),6) AS w6, to_week(to_date('2001-01-01'),7) AS w7;

SELECT to_year_week(to_date('2000-12-31'),0), to_year_week(to_date('2000-12-31'),1), to_year_week(to_date('2000-12-31'),2), to_year_week(to_date('2000-12-31'),3), to_year_week(to_date('2000-12-31'),4), to_year_week(to_date('2000-12-31'),5), to_year_week(to_date('2000-12-31'),6), to_year_week(to_date('2000-12-31'),7);

-- week mode 8,9	
SELECT
    to_date('2016-12-21') + number AS d, 
	  to_week(d, 8) AS week8,
    to_week(d, 9) AS week9, 
    to_year_week(d, 8) AS yearWeek8,
    to_year_week(d, 9) AS yearWeek9
FROM numbers(21);

SELECT to_datetime(to_date('2016-12-22') + number, 'Asia/Istanbul' ) AS d, 
    to_week(d, 8, 'Asia/Istanbul') AS week8, 
    to_week(d, 9, 'Asia/Istanbul') AS week9, 
    to_year_week(d, 8, 'Asia/Istanbul') AS yearWeek8,
    to_year_week(d, 9, 'Asia/Istanbul') AS yearWeek9
FROM numbers(21);

-- to_start_of_week
SELECT
    to_date('2018-12-25') + number AS x,
    to_datetime(x) AS x_t,
    to_start_of_week(x) as w0,
    to_start_of_week(x_t) as wt0,
    to_start_of_week(x, 3) as w3,
    to_start_of_week(x_t, 3) as wt3
FROM numbers(10);

