-- week mode [0,7],  week test case. refer to the mysql test case
SELECT toWeek(to_date('1998-01-01')), toWeek(to_date('1997-01-01')), toWeek(to_date('1998-01-01'), 1),  toWeek(to_date('1997-01-01'), 1);
SELECT toWeek(to_date('1998-12-31')), toWeek(to_date('1997-12-31')), toWeek(to_date('1998-12-31'), 1), toWeek(to_date('1997-12-31'), 1);
SELECT toWeek(to_date('1995-01-01')), toWeek(to_date('1995-01-01'), 1);
SELECT toYearWeek(to_date('1981-12-31'), 1), toYearWeek(to_date('1982-01-01'), 1), toYearWeek(to_date('1982-12-31'), 1), toYearWeek(to_date('1983-01-01'), 1);
SELECT toYearWeek(to_date('1987-01-01'), 1), toYearWeek(to_date('1987-01-01'));	
	
SELECT toWeek(to_date('2000-01-01'),0) AS w2000, toWeek(to_date('2001-01-01'),0) AS w2001, toWeek(to_date('2002-01-01'),0) AS w2002,toWeek(to_date('2003-01-01'),0) AS w2003, toWeek(to_date('2004-01-01'),0) AS w2004, toWeek(to_date('2005-01-01'),0) AS w2005, toWeek(to_date('2006-01-01'),0) AS w2006;
SELECT toWeek(to_date('2000-01-06'),0) AS w2000, toWeek(to_date('2001-01-06'),0) AS w2001, toWeek(to_date('2002-01-06'),0) AS w2002,toWeek(to_date('2003-01-06'),0) AS w2003, toWeek(to_date('2004-01-06'),0) AS w2004, toWeek(to_date('2005-01-06'),0) AS w2005, toWeek(to_date('2006-01-06'),0) AS w2006;
SELECT toWeek(to_date('2000-01-01'),1) AS w2000, toWeek(to_date('2001-01-01'),1) AS w2001, toWeek(to_date('2002-01-01'),1) AS w2002,toWeek(to_date('2003-01-01'),1) AS w2003, toWeek(to_date('2004-01-01'),1) AS w2004, toWeek(to_date('2005-01-01'),1) AS w2005, toWeek(to_date('2006-01-01'),1) AS w2006;
SELECT toWeek(to_date('2000-01-06'),1) AS w2000, toWeek(to_date('2001-01-06'),1) AS w2001, toWeek(to_date('2002-01-06'),1) AS w2002,toWeek(to_date('2003-01-06'),1) AS w2003, toWeek(to_date('2004-01-06'),1) AS w2004, toWeek(to_date('2005-01-06'),1) AS w2005, toWeek(to_date('2006-01-06'),1) AS w2006;
SELECT toYearWeek(to_date('2000-01-01'),0) AS w2000, toYearWeek(to_date('2001-01-01'),0) AS w2001, toYearWeek(to_date('2002-01-01'),0) AS w2002,toYearWeek(to_date('2003-01-01'),0) AS w2003, toYearWeek(to_date('2004-01-01'),0) AS w2004, toYearWeek(to_date('2005-01-01'),0) AS w2005, toYearWeek(to_date('2006-01-01'),0) AS w2006;
SELECT toYearWeek(to_date('2000-01-06'),0) AS w2000, toYearWeek(to_date('2001-01-06'),0) AS w2001, toYearWeek(to_date('2002-01-06'),0) AS w2002,toYearWeek(to_date('2003-01-06'),0) AS w2003, toYearWeek(to_date('2004-01-06'),0) AS w2004, toYearWeek(to_date('2005-01-06'),0) AS w2005, toYearWeek(to_date('2006-01-06'),0) AS w2006;
SELECT toYearWeek(to_date('2000-01-01'),1) AS w2000, toYearWeek(to_date('2001-01-01'),1) AS w2001, toYearWeek(to_date('2002-01-01'),1) AS w2002,toYearWeek(to_date('2003-01-01'),1) AS w2003, toYearWeek(to_date('2004-01-01'),1) AS w2004, toYearWeek(to_date('2005-01-01'),1) AS w2005, toYearWeek(to_date('2006-01-01'),1) AS w2006;
SELECT toYearWeek(to_date('2000-01-06'),1) AS w2000, toYearWeek(to_date('2001-01-06'),1) AS w2001, toYearWeek(to_date('2002-01-06'),1) AS w2002,toYearWeek(to_date('2003-01-06'),1) AS w2003, toYearWeek(to_date('2004-01-06'),1) AS w2004, toYearWeek(to_date('2005-01-06'),1) AS w2005, toYearWeek(to_date('2006-01-06'),1) AS w2006;	
SELECT toWeek(to_date('1998-12-31'),2),toWeek(to_date('1998-12-31'),3), toWeek(to_date('2000-01-01'),2), toWeek(to_date('2000-01-01'),3);
SELECT toWeek(to_date('2000-12-31'),2),toWeek(to_date('2000-12-31'),3);

SELECT toWeek(to_date('1998-12-31'),0) AS w0, toWeek(to_date('1998-12-31'),1) AS w1, toWeek(to_date('1998-12-31'),2) AS w2, toWeek(to_date('1998-12-31'),3) AS w3, toWeek(to_date('1998-12-31'),4) AS w4, toWeek(to_date('1998-12-31'),5) AS w5, toWeek(to_date('1998-12-31'),6) AS w6, toWeek(to_date('1998-12-31'),7) AS w7;
SELECT toWeek(to_date('2000-01-01'),0) AS w0, toWeek(to_date('2000-01-01'),1) AS w1, toWeek(to_date('2000-01-01'),2) AS w2, toWeek(to_date('2000-01-01'),3) AS w3, toWeek(to_date('2000-01-01'),4) AS w4, toWeek(to_date('2000-01-01'),5) AS w5, toWeek(to_date('2000-01-01'),6) AS w6, toWeek(to_date('2000-01-01'),7) AS w7;
SELECT toWeek(to_date('2000-01-06'),0) AS w0, toWeek(to_date('2000-01-06'),1) AS w1, toWeek(to_date('2000-01-06'),2) AS w2, toWeek(to_date('2000-01-06'),3) AS w3, toWeek(to_date('2000-01-06'),4) AS w4, toWeek(to_date('2000-01-06'),5) AS w5, toWeek(to_date('2000-01-06'),6) AS w6, toWeek(to_date('2000-01-06'),7) AS w7;
SELECT toWeek(to_date('2000-12-31'),0) AS w0, toWeek(to_date('2000-12-31'),1) AS w1, toWeek(to_date('2000-12-31'),2) AS w2, toWeek(to_date('2000-12-31'),3) AS w3, toWeek(to_date('2000-12-31'),4) AS w4, toWeek(to_date('2000-12-31'),5) AS w5, toWeek(to_date('2000-12-31'),6) AS w6, toWeek(to_date('2000-12-31'),7) AS w7;
SELECT toWeek(to_date('2001-01-01'),0) AS w0, toWeek(to_date('2001-01-01'),1) AS w1, toWeek(to_date('2001-01-01'),2) AS w2, toWeek(to_date('2001-01-01'),3) AS w3, toWeek(to_date('2001-01-01'),4) AS w4, toWeek(to_date('2001-01-01'),5) AS w5, toWeek(to_date('2001-01-01'),6) AS w6, toWeek(to_date('2001-01-01'),7) AS w7;

SELECT toYearWeek(to_date('2000-12-31'),0), toYearWeek(to_date('2000-12-31'),1), toYearWeek(to_date('2000-12-31'),2), toYearWeek(to_date('2000-12-31'),3), toYearWeek(to_date('2000-12-31'),4), toYearWeek(to_date('2000-12-31'),5), toYearWeek(to_date('2000-12-31'),6), toYearWeek(to_date('2000-12-31'),7);

-- week mode 8,9	
SELECT
    to_date('2016-12-21') + number AS d, 
	  toWeek(d, 8) AS week8,
    toWeek(d, 9) AS week9, 
    toYearWeek(d, 8) AS yearWeek8,
    toYearWeek(d, 9) AS yearWeek9
FROM numbers(21);

SELECT to_datetime(to_date('2016-12-22') + number, 'Europe/Moscow' ) AS d, 
    toWeek(d, 8, 'Europe/Moscow') AS week8, 
    toWeek(d, 9, 'Europe/Moscow') AS week9, 
    toYearWeek(d, 8, 'Europe/Moscow') AS yearWeek8,
    toYearWeek(d, 9, 'Europe/Moscow') AS yearWeek9
FROM numbers(21);

-- toStartOfWeek
SELECT
    to_date('2018-12-25') + number AS x,
    to_datetime(x) AS x_t,
    toStartOfWeek(x) AS w0,
    toStartOfWeek(x_t) AS wt0,
    toStartOfWeek(x, 3) AS w3,
    toStartOfWeek(x_t, 3) AS wt3
FROM numbers(10);

