select minMap(array_join([([1], [null]), ([1], [null])])); -- { serverError 43 }
select maxMap(array_join([([1], [null]), ([1], [null])])); -- { serverError 43 }
select sumMap(array_join([([1], [null]), ([1], [null])])); -- { serverError 43 }
select sumMapWithOverflow(array_join([([1], [null]), ([1], [null])])); -- { serverError 43 }

select minMap(array_join([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select maxMap(array_join([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMap(array_join([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMapWithOverflow(array_join([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
