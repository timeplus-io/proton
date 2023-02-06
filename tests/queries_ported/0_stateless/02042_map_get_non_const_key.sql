SELECT map[key]
FROM
(
    SELECT materialize('key') AS key,  CAST((['key'], ['value']), 'map(string, string)') AS map
);
