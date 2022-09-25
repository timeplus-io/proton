 

select today() < 2018-11-14; -- { serverError 43 }
select to_date('2018-01-01') < '2018-11-14';

select to_date('2018-01-01') < '2018-01-01';
select to_date('2018-01-01') == '2018-01-01';
select to_date('2018-01-01') != '2018-01-01';
select to_date('2018-01-01') < to_date('2018-01-01');
select to_date('2018-01-01') == to_date('2018-01-01');
select to_date('2018-01-01') != to_date('2018-01-01');

select to_date('2018-01-01') < 1;  -- { serverError 43 }
select to_date('2018-01-01') == 1; -- { serverError 43 }
select to_date('2018-01-01') != 1; -- { serverError 43 }


