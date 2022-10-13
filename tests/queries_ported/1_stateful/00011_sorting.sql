SELECT EventTime::DateTime('Europe/Moscow') from table(test.hits) ORDER BY EventTime DESC LIMIT 10
