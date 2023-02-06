SELECT 'left_pad';
SELECT left_pad('abc', 0);
SELECT left_pad('abc', 1);
SELECT left_pad('abc', 2);
SELECT left_pad('abc', 3);
SELECT left_pad('abc', 4);
SELECT left_pad('abc', 5);
SELECT left_pad('abc', 10);

SELECT left_pad('abc', 2, '*');
SELECT left_pad('abc', 4, '*');
SELECT left_pad('abc', 5, '*');
SELECT left_pad('abc', 10, '*');
SELECT left_pad('abc', 2, '*.');
SELECT left_pad('abc', 4, '*.');
SELECT left_pad('abc', 5, '*.');
SELECT left_pad('abc', 10, '*.');

SELECT 'left_pad_utf8';
SELECT left_pad('абвг', 2);
SELECT left_pad_utf8('абвг', 2);
SELECT left_pad('абвг', 4);
SELECT left_pad_utf8('абвг', 4);
SELECT left_pad('абвг', 12, 'ЧАС');
SELECT left_pad_utf8('абвг', 12, 'ЧАС');

SELECT 'right_pad';
SELECT right_pad('abc', 0);
SELECT right_pad('abc', 1);
SELECT right_pad('abc', 2);
SELECT right_pad('abc', 3);
SELECT right_pad('abc', 4);
SELECT right_pad('abc', 5);
SELECT right_pad('abc', 10);

SELECT right_pad('abc', 2, '*');
SELECT right_pad('abc', 4, '*');
SELECT right_pad('abc', 5, '*');
SELECT right_pad('abc', 10, '*');
SELECT right_pad('abc', 2, '*.');
SELECT right_pad('abc', 4, '*.');
SELECT right_pad('abc', 5, '*.');
SELECT right_pad('abc', 10, '*.');

SELECT 'right_pad_utf8';
SELECT right_pad('абвг', 2);
SELECT right_pad_utf8('абвг', 2);
SELECT right_pad('абвг', 4);
SELECT right_pad_utf8('абвг', 4);
SELECT right_pad('абвг', 12, 'ЧАС');
SELECT right_pad_utf8('абвг', 12, 'ЧАС');

SELECT 'numbers';
SELECT right_pad(left_pad(to_string(number), number, '_'), number*2, '^') FROM numbers(7);
