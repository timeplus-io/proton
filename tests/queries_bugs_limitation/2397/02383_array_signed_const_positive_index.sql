-- { echo }

SELECT materialize([[13]])[1::int8];
SELECT materialize([['Hello']])[1::int8];
SELECT materialize([13])[1::int8];
SELECT materialize(['Hello'])[1::int8];

SELECT materialize([[13], [14]])[2::int8];
SELECT materialize([['Hello'], ['world']])[2::int8];
SELECT materialize([13, 14])[2::int8];
SELECT materialize(['Hello', 'world'])[2::int8];

SELECT materialize([[13], [14]])[3::int8];
SELECT materialize([['Hello'], ['world']])[3::int8];
SELECT materialize([13, 14])[3::int8];
SELECT materialize(['Hello', 'world'])[3::int8];

SELECT materialize([[13], [14]])[0::int8];
SELECT materialize([['Hello'], ['world']])[0::int8];
SELECT materialize([13, 14])[0::int8];
SELECT materialize(['Hello', 'world'])[0::int8];

SELECT materialize([[13], [14]])[-1];
SELECT materialize([['Hello'], ['world']])[-1];
SELECT materialize([13, 14])[-1];
SELECT materialize(['Hello', 'world'])[-1];

SELECT materialize([[13], [14]])[-9223372036854775808];
SELECT materialize([['Hello'], ['world']])[-9223372036854775808];
SELECT materialize([13, 14])[-9223372036854775808];
SELECT materialize(['Hello', 'world'])[-9223372036854775808];

SELECT materialize([[to_nullable(13)], [14]])[-9223372036854775808];
SELECT materialize([['Hello'], [to_nullable('world')]])[-9223372036854775808];
SELECT materialize([13, to_nullable(14)])[-9223372036854775808];
SELECT materialize(['Hello', to_low_cardinality('world')])[-9223372036854775808];
