SELECT uniq(UserID), uniq_if(UserID, CounterID = 800784), uniq_if(FUniqID, RegionID = 213) from table(test.hits)
