select to_int64('--1'); -- { serverError 72; }
select to_int64('+-1'); -- { serverError 72; }
select to_int64('++1'); -- { serverError 72; }
select to_int64('++'); -- { serverError 72; }
select to_int64('+'); -- { serverError 72; }
select to_int64('1+1'); -- { serverError 6; }
select to_int64('1-1'); -- { serverError 6; }
select to_int64(''); -- { serverError 32; }
select to_int64('1');
select to_int64('-1');
