select is_finite(0) = 1;
select is_finite(1) = 1;
select is_finite(materialize(0)) = 1;
select is_finite(materialize(1)) = 1;
select is_finite(1/0) = 0;
select is_finite(-1/0) = 0;
select is_finite(0/0) = 0;
select is_finite(inf) = 0;
select is_finite(-inf) = 0;
select is_finite(nan) = 0;

select is_infinite(0) = 0;
select is_infinite(1) = 0;
select is_infinite(materialize(0)) = 0;
select is_infinite(materialize(1)) = 0;
select is_infinite(1/0) = 1;
select is_infinite(-1/0) = 1;
select is_infinite(0/0) = 0;
select is_infinite(inf) = 1;
select is_infinite(-inf) = 1;
select is_infinite(nan) = 0;


select is_nan(0) = 0;
select is_nan(1) = 0;
select is_nan(materialize(0)) = 0;
select is_nan(materialize(1)) = 0;
select is_nan(1/0) = 0;
select is_nan(-1/0) = 0;
select is_nan(0/0) = 1;
select is_nan(inf) = 0;
select is_nan(-inf) = 0;
select is_nan(nan) = 1;
