SELECT k, any(u) AS u, unique(us) FROM (SELECT split_by_string('/',URL)[3] AS k, uniq(UserID) AS u, uniq_state(UserID) AS us FROM test.hits GROUP BY k) GROUP BY k ORDER BY u DESC, k ASC LIMIT 100
