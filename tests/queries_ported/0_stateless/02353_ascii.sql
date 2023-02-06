SELECT ascii('234');
SELECT ascii('');
SELECT ascii(materialize('234'));
SELECT ascii(materialize(''));
SELECT ascii(to_string(number) || 'abc') from numbers(10);
