select to_fixed_string(unhex('202005295555'), 15) > unhex('20200529') r;
select materialize(to_fixed_string(unhex('202005295555'), 15)) > unhex('20200529') r;
