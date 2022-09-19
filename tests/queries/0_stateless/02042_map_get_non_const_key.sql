SELECT map[key]
FROM
(
    SELECT materialize('key') AS key,  CAST((['key'], ['value']), 'Map(string, string)') AS map
);
