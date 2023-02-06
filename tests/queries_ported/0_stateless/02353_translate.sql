SELECT translate('Hello? world.', '.?', '!,');
SELECT translate('gtcttgcaag', 'ACGTacgt', 'TGCAtgca');
SELECT translate(to_string(number), '0123456789', 'abcdefghij') FROM numbers(987654, 5);

SELECT translate_utf8('HôtelGenèv', 'Ááéíóúôè', 'aaeiouoe');
SELECT translate_utf8('中文内码', '久标准中文内码', 'ユニコードとは');
SELECT translate_utf8(to_string(number), '1234567890', 'ዩय𐑿𐐏নՅðй¿ค') FROM numbers(987654, 5);

SELECT translate('abc', '', '');
SELECT translate_utf8('abc', '', '');

SELECT translate('abc', 'Ááéíóúôè', 'aaeiouoe'); -- { serverError 36 }
SELECT translate_utf8('abc', 'efg', ''); -- { serverError 36 }
