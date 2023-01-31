select round(1000 * ngram_search_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngram_search_utf8(materialize(''), materialize('')))=round(1000 * ngram_search_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абв'), materialize('')))=round(1000 * ngram_search_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize(''), materialize('абв')))=round(1000 * ngram_search_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), materialize('абвгдеёжз')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), materialize('абвгдеёж')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), materialize('гдеёзд')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), materialize('ёёёёёёёё')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngram_search_utf8('', materialize('')))=round(1000 * ngram_search_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('абв', materialize('')))=round(1000 * ngram_search_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('', materialize('абв')))=round(1000 * ngram_search_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('абвгдеёжз', materialize('абвгдеёжз')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('абвгдеёжз', materialize('абвгдеёж')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('абвгдеёжз', materialize('гдеёзд')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngram_search_utf8('абвгдеёжз', materialize('ёёёёёёёё')))=round(1000 * ngram_search_utf8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngram_search_utf8('', ''));
select round(1000 * ngram_search_utf8('абв', ''));
select round(1000 * ngram_search_utf8('', 'абв'));
select round(1000 * ngram_search_utf8('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * ngram_search_utf8('абвгдеёжз', 'абвгдеёж'));
select round(1000 * ngram_search_utf8('абвгдеёжз', 'гдеёзд'));
select round(1000 * ngram_search_utf8('абвгдеёжз', 'ёёёёёёёё'));
SET query_mode = 'table';
drop stream if exists test_entry_distance;
create stream test_entry_distance (Title string) engine = Memory;
insert into test_entry_distance values ('привет как дела?... Херсон'), ('привет как дела клип - Яндекс.Видео'), ('привет'), ('пап привет как дела - Яндекс.Видео'), ('привет братан как дела - Яндекс.Видео'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://metrica.yandex.com/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_utf8(Title, 'yandex') as distance, Title;


select round(1000 * ngram_search_case_insensitive_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngram_search_case_insensitive_utf8(materialize(''),materialize(''))) = round(1000 * ngram_search_case_insensitive_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абв'),materialize(''))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize(''), materialize('абв'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвГДЕёжз'), materialize('АбвгдЕёжз'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('аБВГдеёЖз'), materialize('АбвГдеёж'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), materialize('гдеёЗД'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngram_search_case_insensitive_utf8('', materialize(''))) = round(1000 * ngram_search_case_insensitive_utf8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('абв',materialize(''))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('', materialize('абв'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('абвГДЕёжз', materialize('АбвгдЕёжз'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('аБВГдеёЖз', materialize('АбвГдеёж'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('абвгдеёжз', materialize('гдеёЗД'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive_utf8('абвгдеёжз', materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngram_search_case_insensitive_utf8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;


select round(1000 * ngram_search_case_insensitive_utf8('', ''));
select round(1000 * ngram_search_case_insensitive_utf8('абв', ''));
select round(1000 * ngram_search_case_insensitive_utf8('', 'абв'));
select round(1000 * ngram_search_case_insensitive_utf8('абвГДЕёжз', 'АбвгдЕЁжз'));
select round(1000 * ngram_search_case_insensitive_utf8('аБВГдеёЖз', 'АбвГдеёж'));
select round(1000 * ngram_search_case_insensitive_utf8('абвгдеёжз', 'гдеёЗД'));
select round(1000 * ngram_search_case_insensitive_utf8('АБВГДеёжз', 'ЁЁЁЁЁЁЁЁ'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'YanDEX') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive_utf8(Title, 'приВЕТ КАк ДеЛа КлИп - яндеКс.видео') as distance, Title;


select round(1000 * ngram_search(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngram_search(materialize(''),materialize('')))=round(1000 * ngram_search(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abc'),materialize('')))=round(1000 * ngram_search(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize(''), materialize('abc')))=round(1000 * ngram_search(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), materialize('abcdefgh')))=round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), materialize('abcdefg')))=round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), materialize('defgh')))=round(1000 * ngram_search(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search(materialize('abcdefgh'), materialize('aaaaaaaa')))=round(1000 * ngram_search(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngram_search('',materialize('')))=round(1000 * ngram_search(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search('abc', materialize('')))=round(1000 * ngram_search(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search('', materialize('abc')))=round(1000 * ngram_search(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search('abcdefgh', materialize('abcdefgh')))=round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search('abcdefgh', materialize('abcdefg')))=round(1000 * ngram_search(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngram_search('abcdefgh', materialize('defgh')))=round(1000 * ngram_search(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search('abcdefgh', materialize('aaaaaaaa')))=round(1000 * ngram_search(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;


select round(1000 * ngram_search('', ''));
select round(1000 * ngram_search('abc', ''));
select round(1000 * ngram_search('', 'abc'));
select round(1000 * ngram_search('abcdefgh', 'abcdefgh'));
select round(1000 * ngram_search('abcdefgh', 'abcdefg'));
select round(1000 * ngram_search('abcdefgh', 'defgh'));
select round(1000 * ngram_search('abcdefghaaaaaaaaaa', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search(Title, 'yandex') as distance, Title;

select round(1000 * ngram_search_case_insensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngram_search_case_insensitive(materialize(''), materialize('')))=round(1000 * ngram_search_case_insensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abc'), materialize('')))=round(1000 * ngram_search_case_insensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize(''), materialize('abc')))=round(1000 * ngram_search_case_insensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abCdefgH'), materialize('Abcdefgh')))=round(1000 * ngram_search_case_insensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('abcdefgh'), materialize('abcdeFG')))=round(1000 * ngram_search_case_insensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('AAAAbcdefgh'), materialize('defgh')))=round(1000 * ngram_search_case_insensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive(materialize('ABCdefgH'), materialize('aaaaaaaa')))=round(1000 * ngram_search_case_insensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngram_search_case_insensitive('', materialize('')))=round(1000 * ngram_search_case_insensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('abc', materialize('')))=round(1000 * ngram_search_case_insensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('', materialize('abc')))=round(1000 * ngram_search_case_insensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('abCdefgH', materialize('Abcdefgh')))=round(1000 * ngram_search_case_insensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('abcdefgh', materialize('abcdeFG')))=round(1000 * ngram_search_case_insensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('AAAAbcdefgh', materialize('defgh')))=round(1000 * ngram_search_case_insensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngram_search_case_insensitive('ABCdefgH', materialize('aaaaaaaa')))=round(1000 * ngram_search_case_insensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngram_search_case_insensitive('', ''));
select round(1000 * ngram_search_case_insensitive('abc', ''));
select round(1000 * ngram_search_case_insensitive('', 'abc'));
select round(1000 * ngram_search_case_insensitive('abCdefgH', 'Abcdefgh'));
select round(1000 * ngram_search_case_insensitive('abcdefgh', 'abcdeFG'));
select round(1000 * ngram_search_case_insensitive('AAAAbcdefgh', 'defgh'));
select round(1000 * ngram_search_case_insensitive('ABCdefgHaAaaaAaaaAA', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngram_search_case_insensitive(Title, 'YanDEX') as distance, Title;

drop stream if exists test_entry_distance;
