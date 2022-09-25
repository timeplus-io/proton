SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparated;
SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSV;

SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedWithNames;
SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVWithNames;

SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedWithNamesAndTypes;
SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVWithNamesAndTypes;

SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedRaw;
SELECT array_join([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVRaw;
