select split_by_char(',', '1,2,3');
select split_by_char(',', '1,2,3', -1);
select split_by_char(',', '1,2,3', 0);
select split_by_char(',', '1,2,3', 1);
select split_by_char(',', '1,2,3', 2);
select split_by_char(',', '1,2,3', 3);
select split_by_char(',', '1,2,3', 4);

select split_by_regexp('[ABC]', 'oneAtwoBthreeC');
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', -1);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 0);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 1);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 2);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 3);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 4);
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', 5);

SELECT alpha_tokens('abca1abc');
SELECT alpha_tokens('abca1abc', -1);
SELECT alpha_tokens('abca1abc', 0);
SELECT alpha_tokens('abca1abc', 1);
SELECT alpha_tokens('abca1abc', 2);
SELECT alpha_tokens('abca1abc', 3);

SELECT split_by_alpha('abca1abc');

SELECT split_by_non_alpha('  1!  a,  b.  ');
SELECT split_by_non_alpha('  1!  a,  b.  ', -1);
SELECT split_by_non_alpha('  1!  a,  b.  ',  0);
SELECT split_by_non_alpha('  1!  a,  b.  ',  1);
SELECT split_by_non_alpha('  1!  a,  b.  ',  2);
SELECT split_by_non_alpha('  1!  a,  b.  ',  3);
SELECT split_by_non_alpha('  1!  a,  b.  ',  4);

SELECT split_by_whitespace('  1!  a,  b.  ');
SELECT split_by_whitespace('  1!  a,  b.  ', -1);
SELECT split_by_whitespace('  1!  a,  b.  ', 0);
SELECT split_by_whitespace('  1!  a,  b.  ', 1);
SELECT split_by_whitespace('  1!  a,  b.  ', 2);
SELECT split_by_whitespace('  1!  a,  b.  ', 3);
SELECT split_by_whitespace('  1!  a,  b.  ', 4);

SELECT split_by_string(', ', '1, 2 3, 4,5, abcde');
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', -1);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 0);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 1);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 2);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 3);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 4);
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', 5);


select split_by_char(',', '1,2,3', ''); -- { serverError 43 }
select split_by_regexp('[ABC]', 'oneAtwoBthreeC', ''); -- { serverError 43 }
SELECT alpha_tokens('abca1abc', ''); -- { serverError 43 }
SELECT split_by_alpha('abca1abc', ''); -- { serverError 43 }
SELECT split_by_non_alpha('  1!  a,  b.  ',  ''); -- { serverError 43 }
SELECT split_by_whitespace('  1!  a,  b.  ', ''); -- { serverError 43 }
SELECT split_by_string(', ', '1, 2 3, 4,5, abcde', ''); -- { serverError 43 }