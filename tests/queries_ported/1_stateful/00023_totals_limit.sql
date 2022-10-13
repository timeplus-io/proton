SET output_format_write_statistics = 0;
SELECT goals_alias.ID AS `ym:s:goalDimension`, uniq_if(UserID, (UserID != 0) AND (`_uniq_Goals` = 1))  FROM table(test.visits) ARRAY JOIN Goals AS goals_alias,  array_enumerate_uniq(Goals.ID)   AS `_uniq_Goals`  WHERE (CounterID = 842440) GROUP BY `ym:s:goalDimension` WITH TOTALS ORDER BY `ym:s:goalDimension` LIMIT 0, 1 FORMAT JSONCompact;
