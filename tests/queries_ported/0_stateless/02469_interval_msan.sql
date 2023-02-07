SELECT now() + 1::int128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::int256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::uint128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() + 1::uint256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT now() - 1::int128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::int256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::uint128; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT now() - 1::uint256; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT now() + INTERVAL 1::int128 SECOND - now();
SELECT now() + INTERVAL 1::int256 SECOND - now();
SELECT now() + INTERVAL 1::uint128 SECOND - now();
SELECT now() + INTERVAL 1::uint256 SECOND - now();

SELECT today() + INTERVAL 1::int128 DAY - today();
SELECT today() + INTERVAL 1::int256 DAY - today();
SELECT today() + INTERVAL 1::uint128 DAY - today();
SELECT today() + INTERVAL 1::uint256 DAY - today();
