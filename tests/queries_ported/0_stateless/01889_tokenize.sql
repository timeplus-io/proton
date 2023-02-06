SET allow_experimental_nlp_functions = 1;

SELECT split_by_non_alpha('It is quite a wonderful day, isn\'t it?');
SELECT split_by_non_alpha('There is.... so much to learn!');
SELECT split_by_non_alpha('22:00 email@tut.by');
SELECT split_by_non_alpha('Токенизация каких-либо других языков?');

SELECT split_by_whitespace('It is quite a wonderful day, isn\'t it?');
SELECT split_by_whitespace('There is.... so much to learn!');
SELECT split_by_whitespace('22:00 email@tut.by');
SELECT split_by_whitespace('Токенизация каких-либо других языков?');
