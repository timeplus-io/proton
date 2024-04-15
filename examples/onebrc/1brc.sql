SET format_csv_delimiter = ';';

SELECT 
    concat('{', array_string_concat(group_array(formatted_result), ', '), '}') AS final_output
FROM (
    SELECT 
        format('{}={}/{}/{}', city, to_string(to_decimal(min(temperature), 1)), to_string(to_decimal(avg(temperature), 1)), to_string(to_decimal(max(temperature), 1))) AS formatted_result
    FROM file('measurements.txt', 'CSV', 'city string, temperature float32')
    GROUP BY city
    ORDER BY city
)
