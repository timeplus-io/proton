select starts_with([], []);
select starts_with([1], []);
select starts_with([], [1]);
select '-'; 

select starts_with([NULL], [NULL]);
select starts_with([NULL], []);
select starts_with([], [NULL]);
select starts_with([NULL, 1], [NULL]);
select starts_with([NULL, 1], [1]);
select '-'; 

select starts_with([1, 2, 3, 4], [1, 2, 3]);
select starts_with([1, 2, 3, 4], [1, 2, 4]);
select starts_with(['a', 'b', 'c'], ['a', 'b']);
select starts_with(['a', 'b', 'c'], ['b']);
select '-'; 

select ends_with([], []);
select ends_with([1], []);
select ends_with([], [1]);
select '-'; 

select ends_with([NULL], [NULL]);
select ends_with([NULL], []);
select ends_with([], [NULL]);
select ends_with([1, NULL], [NULL]);
select ends_with([NULL, 1], [NULL]);
select '-'; 

select ends_with([1, 2, 3, 4], [3, 4]);
select ends_with([1, 2, 3, 4], [3]);
select '-'; 

select starts_with([1], empty_array_uint8());
select ends_with([1], empty_array_uint8());
