select append_trailing_char_if_absent('', 'a') = '';
select append_trailing_char_if_absent('a', 'a') = 'a';
select append_trailing_char_if_absent('a', 'b') = 'ab';
select append_trailing_char_if_absent(materialize(''), 'a') = materialize('');
select append_trailing_char_if_absent(materialize('a'), 'a') = materialize('a');
select append_trailing_char_if_absent(materialize('a'), 'b') = materialize('ab');
