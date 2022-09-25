SELECT (number % 2) <> 0 ? [1, 2] : [3, 4, 5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([1, 2]) : [3, 4, 5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [1, 2] : materialize([3, 4, 5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([1, 2]) : materialize([3, 4, 5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? [1, 2] : emptyArrayInt64() AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [1, 2] : range(number) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? range(number) : range(to_uint64(10 - number)) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? [256, 257] : [300, -500000, 500] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [1, 2] : [3, 4, -5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [256] : [3, 4, -5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [0xFFFFFFFF] : [-1] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? materialize([256, 257]) : [300, -500000, 500] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([1, 2]) : [3, 4, -5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([256]) : [3, 4, -5] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([0xFFFFFFFF]) : [-1] AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? [256, 257] : materialize([300, -500000, 500]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [1, 2] : materialize([3, 4, -5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [256] : materialize([3, 4, -5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? [0xFFFFFFFF] : materialize([-1]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? materialize([256, 257]) :  materialize([300, -500000, 500]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([1, 2]) :  materialize([3, 4, -5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([256]) :  materialize([3, 4, -5]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
SELECT (number % 2) <> 0 ? materialize([0xFFFFFFFF]) :  materialize([-1]) AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;

SELECT (number % 2) <> 0 ? [1.1, 2] : emptyArrayInt32() AS res FROM system.numbers LIMIT 10 FORMAT TabSeparatedWithNamesAndTypes;
