-- When left and right element types are compatible, but the particular value
-- on the right is not in the range of the left type, it should be ignored.
select (to_uint8(1)) in (-1);
select (to_uint8(0)) in (-1);
select (to_uint8(255)) in (-1);

select [to_uint8(1)] in [-1];
select [to_uint8(0)] in [-1];
select [to_uint8(255)] in [-1];

-- When left and right element types are not compatible, we should get an error.
select (to_uint8(1)) in ('a'); -- { serverError 53 }
select [to_uint8(1)] in ['a']; -- { serverError 53 }
