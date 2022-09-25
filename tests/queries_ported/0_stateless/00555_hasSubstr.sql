select has_substr([], []);
select has_substr([], [1]);
select has_substr([], [NULL]);
select has_substr([Null], [Null]);
select has_substr([Null], [Null, 1]);
select has_substr([1], []);
select has_substr([1], [Null]);
select has_substr([1, Null], [Null]);
select has_substr([1, Null, 3, 4, Null, 5, 7], [3, 4, Null]);
select has_substr([1, Null], [3, 4, Null]);
select '-';


select has_substr([1], empty_array_uint8());
select '-';

select has_substr([1, 2, 3, 4], [1, 3]);
select has_substr([1, 2, 3, 4], [1, 3, 5]);
select has_substr([-128, 1., 512], [1.]);
select has_substr([-128, 1.0, 512], [.3]);
select '-';

select has_substr(['a'], ['a']);
select has_substr(['a', 'b'], ['a', 'c']);
select has_substr(['a', 'c', 'b'], ['a', 'c']);
select '-';

select has_substr([1], ['a']); -- { serverError 386 }
select has_substr([[1, 2], [3, 4]], ['a', 'c']); -- { serverError 386 }
select has_substr([[1, 2], [3, 4], [5, 8]], [[3, 4]]);
select has_substr([[1, 2], [3, 4], [5, 8]], [[3, 4], [5, 8]]);
select has_substr([[1, 2], [3, 4], [5, 8]], [[1, 2], [5, 8]]);
