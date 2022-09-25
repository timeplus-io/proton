select lower('aaaaaaaaaaaaaaa012345789,.!aaaa' as str) = str;
select lower_utf8('aaaaaaaaaaaaaaa012345789,.!aaaa' as str) = str;
select lower('AaAaAaAaAaAaAaA012345789,.!aAaA') = 'aaaaaaaaaaaaaaa012345789,.!aaaa';
select lower_utf8('AaAaAaAaAaAaAaA012345789,.!aAaA') = 'aaaaaaaaaaaaaaa012345789,.!aaaa';

select upper('AAAAAAAAAAAAAAA012345789,.!AAAA' as str) = str;
select upper_utf8('AAAAAAAAAAAAAAA012345789,.!AAAA' as str) = str;
select upper('AaAaAaAaAaAaAaA012345789,.!aAaA') = 'AAAAAAAAAAAAAAA012345789,.!AAAA';
select upper_utf8('AaAaAaAaAaAaAaA012345789,.!aAaA') = 'AAAAAAAAAAAAAAA012345789,.!AAAA';

select sum(to_uint8(lower(materialize('aaaaaaaaaaaaaaa012345789,.!aaaa') as str) = str)) = count() from system.one array join range(16384) as n;
select sum(to_uint8(to_uint8(lower(materialize('aaaaaaaaaaaaaaa012345789,.!aaaa') as str) = str))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(lower(materialize('AaAaAaAaAaAaAaA012345789,.!aAaA')) = materialize('aaaaaaaaaaaaaaa012345789,.!aaaa'))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(lower(materialize('AaAaAaAaAaAaAaA012345789,.!aAaA')) = materialize('aaaaaaaaaaaaaaa012345789,.!aaaa'))) = count() from system.one array join range(16384) as n;

select sum(to_uint8(upper(materialize('AAAAAAAAAAAAAAA012345789,.!AAAA') as str) = str)) = count() from system.one array join range(16384) as n;
select sum(to_uint8(upper_utf8(materialize('AAAAAAAAAAAAAAA012345789,.!AAAA') as str) = str)) = count() from system.one array join range(16384) as n;
select sum(to_uint8(upper(materialize('AaAaAaAaAaAaAaA012345789,.!aAaA')) = materialize('AAAAAAAAAAAAAAA012345789,.!AAAA'))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(upper_utf8(materialize('AaAaAaAaAaAaAaA012345789,.!aAaA')) = materialize('AAAAAAAAAAAAAAA012345789,.!AAAA'))) = count() from system.one array join range(16384) as n;

select lower('aaaaАБВГAAAAaaAA') = 'aaaaАБВГaaaaaaaa';
select upper('aaaaАБВГAAAAaaAA') = 'AAAAАБВГAAAAAAAA';
select lower_utf8('aaaaАБВГAAAAaaAA') = 'aaaaабвгaaaaaaaa';
select upper_utf8('aaaaАБВГAAAAaaAA') = 'AAAAАБВГAAAAAAAA';

select sum(to_uint8(lower(materialize('aaaaАБВГAAAAaaAA')) = materialize('aaaaАБВГaaaaaaaa'))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(upper(materialize('aaaaАБВГAAAAaaAA')) = materialize('AAAAАБВГAAAAAAAA'))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(lower(materialize('aaaaАБВГAAAAaaAA')) = materialize('aaaaабвгaaaaaaaa'))) = count() from system.one array join range(16384) as n;
select sum(to_uint8(upper_utf8(materialize('aaaaАБВГAAAAaaAA')) = materialize('AAAAАБВГAAAAAAAA'))) = count() from system.one array join range(16384) as n;
