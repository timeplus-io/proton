SET output_format_write_statistics = 0;

SELECT count(), array_join([1, 2, 3]) AS n GROUP BY n WITH TOTALS ORDER BY n LIMIT 1 FORMAT JSON;
