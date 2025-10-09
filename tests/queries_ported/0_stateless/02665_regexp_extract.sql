select regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
select regexp_extract('100-200', '(\\d+)-(\\d+)');
select regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
select regexp_extract('100-200', '(\\d+).*', 1);
select regexp_extract('100-200', '([a-z])', 1);
select regexp_extract(null, '([a-z])', 1);
select regexp_extract('100-200', null, 1);
select regexp_extract('100-200', '([a-z])', null);

select regexp_extract(materialize('100-200'), '(\\d+)-(\\d+)');
select regexp_extract(materialize('100-200'), '(\\d+)-(\\d+)', 1);
select regexp_extract(materialize('100-200'), '(\\d+)-(\\d+)', 2);
select regexp_extract(materialize('100-200'), '(\\d+).*', 1);
select regexp_extract(materialize('100-200'), '([a-z])', 1);
select regexp_extract(materialize(null), '([a-z])', 1);
select regexp_extract(materialize('100-200'), null, 1);
select regexp_extract(materialize('100-200'), '([a-z])', null);

select regexp_extract('100-200', '(\\d+)-(\\d+)', materialize(1));
select regexp_extract('100-200', '(\\d+)-(\\d+)', materialize(2));
select regexp_extract('100-200', '(\\d+).*', materialize(1));
select regexp_extract('100-200', '([a-z])', materialize(1));
select regexp_extract(null, '([a-z])', materialize(1));
select regexp_extract('100-200', null, materialize(1));
select regexp_extract('100-200', '([a-z])', materialize(null));

select regexp_extract(materialize('100-200'), '(\\d+)-(\\d+)', materialize(1));
select regexp_extract(materialize('100-200'), '(\\d+)-(\\d+)', materialize(2));
select regexp_extract(materialize('100-200'), '(\\d+).*', materialize(1));
select regexp_extract(materialize('100-200'), '([a-z])', materialize(1));
select regexp_extract(materialize(null), '([a-z])', materialize(1));
select regexp_extract(materialize('100-200'), null, materialize(1));
select regexp_extract(materialize('100-200'), '([a-z])', materialize(null));


select regexp_extract('100-200'); -- { serverError 42 }
select regexp_extract('100-200', '(\\d+)-(\\d+)', 1, 2); -- { serverError 42 }
select regexp_extract(cast('100-200' as fixed_string(10)), '(\\d+)-(\\d+)', 1); -- { serverError 43 }
select regexp_extract('100-200', cast('(\\d+)-(\\d+)' as fixed_string(20)), 1); -- { serverError 43 }
select regexp_extract('100-200', materialize('(\\d+)-(\\d+)'), 1); -- { serverError 44 }
