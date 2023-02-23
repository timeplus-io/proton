-- Tags: no-fasttest

SELECT region_to_name(number::uint32, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'xy') FROM numbers(13); -- { serverError 1000 }

SELECT region_to_name(number::uint32, 'en'), regionToCity(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToArea(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToDistrict(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToCountry(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToContinent(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToTopContinent(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(number::uint32, 'en'), regionToPopulation(number::uint32) AS id, region_to_name(id, 'en') FROM numbers(13);
SELECT region_to_name(n1.number::uint32, 'en') || (regionIn(n1.number::uint32, n2.number::uint32) ? ' is in ' : ' is not in ') || region_to_name(n2.number::uint32, 'en') FROM numbers(13) AS n1 CROSS JOIN numbers(13) AS n2;
SELECT regionHierarchy(number::uint32) AS arr, array_map(id -> region_to_name(id, 'en'), arr) FROM numbers(13);
