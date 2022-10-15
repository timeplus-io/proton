SELECT EventDate, uniq_exact(UserID), length(groupUniqArray(UserID)), arrayUniq(group_array(UserID)) from table(test.hits) WHERE CounterID = 1704509 GROUP BY EventDate ORDER BY EventDate;
