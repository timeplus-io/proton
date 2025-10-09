SELECT
    to_Float64('1.7091'),
    to_Float64('1.5008753E7'),
    to_Float64('6e-09'),
    to_Float64('6.000000000000001e-9'),
    to_Float32('1.7091'),
    to_Float32('1.5008753E7'),
    to_Float32('6e-09'),
    to_Float32('6.000000000000001e-9')
SETTINGS precise_float_parsing = 0;

SELECT
    to_Float64('1.7091'),
    to_Float64('1.5008753E7'),
    to_Float64('6e-09'),
    to_Float64('6.000000000000001e-9'),
    to_Float32('1.7091'),
    to_Float32('1.5008753E7'),
    to_Float32('6e-09'),
    to_Float32('6.000000000000001e-9')
SETTINGS precise_float_parsing = 1;
