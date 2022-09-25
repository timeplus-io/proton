select has_all([], []);
select has_all([], [1]);
select has_all([], [NULL]);
select has_all([Null], [Null]);
select has_all([Null], [Null, 1]);
select has_all([1], []);
select has_all([1], [Null]);
select has_all([1, Null], [Null]);
select '-';

select has_any([], []);
select has_any([], [1]);
select has_any([], [NULL]);
select has_any([Null], [Null]);
select has_any([Null], [Null, 1]);
select has_any([1], []);
select has_any([1], [Null]);
select has_any([1, Null], [Null]);
select '-';

select has_all([1], empty_array_uint8());
select has_any([1], empty_array_uint8());
select '-';

select has_any([1, 2, 3, 4], [5, 6]);
select has_any([1, 2, 3, 4], [1, 3, 5]);
select has_any([1, 2, 3, 4], [1, 3]);
select has_all([1, 2, 3, 4], [1, 3]);
select has_all([1, 2, 3, 4], [1, 3, 5]);
select has_any([-128, 1., 512], [1.]);
select has_any([-128, 1.0, 512], [.3]);
select has_all([-128, 1.0, 512], [1.0]);
select has_all([-128, 1.0, 512], [1.0, 513]);
select '-';

select has_any(['a'], ['a']);
select has_all(['a'], ['a']);
select has_any(['a', 'b'], ['a', 'c']);
select has_all(['a', 'b'], ['a', 'c']);
select '-';

select has_any([1], ['a']); -- { serverError 386 }
select has_all([1], ['a']); -- { serverError 386 }
select has_all([[1, 2], [3, 4]], ['a', 'c']); -- { serverError 386 }
select has_any([[1, 2], [3, 4]], ['a', 'c']); -- { serverError 386 }
select '-';

select has_all([[1, 2], [3, 4]], [[1, 2], [3, 5]]);
select has_all([[1, 2], [3, 4]], [[1, 2], [1, 2]]);
select has_any([[1, 2], [3, 4]], [[1, 2], [3, 5]]);
select has_any([[1, 2], [3, 4]], [[1, 3], [4, 2]]);

