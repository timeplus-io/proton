--
-- count_substrings
--
select '';
select '# count_substrings';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'empty';
select count_substrings('', '.');
select count_substrings('', '');
select count_substrings('.', '');

select 'char';
select count_substrings('foobar.com', '.');
select count_substrings('www.foobar.com', '.');
select count_substrings('.foobar.com.', '.');

select 'word';
select count_substrings('foobar.com', 'com');
select count_substrings('com.foobar', 'com');
select count_substrings('foo.com.bar', 'com');
select count_substrings('com.foobar.com', 'com');
select count_substrings('com.foo.com.bar.com', 'com');

select 'intersect';
select count_substrings('aaaa', 'aa');

select '';
select 'CountSubstringsImpl::vectorVector';
select count_substrings(to_string(number), to_string(number)) from numbers(1);
select count_substrings(concat(to_string(number), '000111'), to_string(number)) from numbers(1);
select count_substrings(concat(to_string(number), '000111001'), to_string(number)) from numbers(1);
select 'intersect', count_substrings(concat(to_string(number), '0000000'), '00') from numbers(1) format CSV;

select '';
select 'CountSubstringsImpl::constantVector';
select count_substrings('100', to_string(number)) from numbers(3);
select count_substrings('0100', to_string(number)) from numbers(1);
select count_substrings('010000', to_string(number)) from numbers(1);
select 'intersect', count_substrings('00000000', repeat(to_string(number), 2)) from numbers(1) format CSV;

select '';
select 'CountSubstringsImpl::vectorConstant';
select count_substrings(to_string(number), '1') from system.numbers limit 3 offset 9;
select count_substrings(concat(to_string(number), '000111'), '1') from numbers(1);
select count_substrings(concat(to_string(number), '000111001'), '1') from numbers(1);
select 'intersect', count_substrings(repeat(to_string(number), 8), '00') from numbers(1) format CSV;

--
-- count_substrings_case_insensitive
--
select '';
select '# count_substrings_case_insensitive';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'char';
select count_substrings_case_insensitive('aba', 'B');
select count_substrings_case_insensitive('bab', 'B');
select count_substrings_case_insensitive('BaBaB', 'b');

select 'word';
select count_substrings_case_insensitive('foobar.com', 'COM');
select count_substrings_case_insensitive('com.foobar', 'COM');
select count_substrings_case_insensitive('foo.com.bar', 'COM');
select count_substrings_case_insensitive('com.foobar.com', 'COM');
select count_substrings_case_insensitive('com.foo.com.bar.com', 'COM');

select 'intersect';
select count_substrings_case_insensitive('aaaa', 'AA');

select '';
select 'CountSubstringsImpl::vectorVector';
select count_substrings_case_insensitive(upper(char(number)), lower(char(number))) from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive(concat(to_string(number), 'aaa111'), char(number)) from numbers(100) where number = 0x41;
select count_substrings_case_insensitive(concat(to_string(number), 'aaa111aa1'), char(number)) from numbers(100) where number = 0x41;

select '';
select 'CountSubstringsImpl::constantVector';
select count_substrings_case_insensitive('aab', char(number)) from numbers(100) where number >= 0x41 and number <= 0x43; -- A..C
select count_substrings_case_insensitive('abaa', char(number)) from numbers(100) where number = 0x41;
select count_substrings_case_insensitive('abaaaa', char(number)) from numbers(100) where number = 0x41;

select '';
select 'CountSubstringsImpl::vectorConstant';
select count_substrings_case_insensitive(char(number), 'a') from numbers(100) where number >= 0x41 and number <= 0x43;

--
-- count_substrings_case_insensitive_utf8
--
select '';
select '# count_substrings_case_insensitive_utf8';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'char';
select count_substrings_case_insensitive_utf8('фуу', 'Ф');
select count_substrings_case_insensitive_utf8('ФуФ', 'ф');
select count_substrings_case_insensitive_utf8('ФуФуФ', 'ф');

select 'word';
select count_substrings_case_insensitive_utf8('подстрока.рф', 'РФ');
select count_substrings_case_insensitive_utf8('рф.подстрока', 'рф');
select count_substrings_case_insensitive_utf8('подстрока.рф.подстрока', 'РФ');
select count_substrings_case_insensitive_utf8('рф.подстрока.рф', 'рф');
select count_substrings_case_insensitive_utf8('рф.подстрока.рф.подстрока.рф', 'РФ');

select 'intersect';
select count_substrings_case_insensitive_utf8('яяяя', 'ЯЯ');

select '';
select 'CountSubstringsImpl::vectorVector';
-- can't use any char, since this will not make valid UTF8
-- for the haystack we use number as-is, for needle we just add dependency from number to go to vectorVector code
select count_substrings_case_insensitive_utf8(upper_utf8(concat(char(number), 'я')), lower_utf8(concat(substring_utf8(char(number), 2), 'Я'))) from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive_utf8(concat(to_string(number), 'ЯЯЯ111'), concat(substring_utf8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive_utf8(concat(to_string(number), 'яяя111яя1'), concat(substring_utf8(char(number), 2), 'Я')) from numbers(100) where number = 0x41; -- A
select 'intersect', count_substrings_case_insensitive_utf8(concat(to_string(number), 'яяяяяяяя'), concat(substring_utf8(char(number), 2), 'Яя')) from numbers(100) where number = 0x41 format CSV; -- A

select '';
select 'CountSubstringsImpl::constantVector';
select count_substrings_case_insensitive_utf8('ЯЯb', concat(substring_utf8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive_utf8('ЯbЯЯ', concat(substring_utf8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive_utf8('ЯbЯЯЯЯ', concat(substring_utf8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select 'intersect', count_substrings_case_insensitive_utf8('ЯЯЯЯЯЯЯЯ', concat(substring_utf8(char(number), 2), 'Яя')) from numbers(100) where number = 0x41 format CSV; -- A

select '';
select 'CountSubstringsImpl::vectorConstant';
select count_substrings_case_insensitive_utf8(concat(char(number), 'я'), 'Я') from numbers(100) where number = 0x41; -- A
select count_substrings_case_insensitive_utf8(concat(char(number), 'б'), 'Я') from numbers(100) where number = 0x41; -- A
select 'intersect', count_substrings_case_insensitive_utf8(concat(char(number), repeat('я', 8)), 'яЯ') from numbers(100) where number = 0x41 format CSV; -- A
