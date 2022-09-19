SELECT number % 4 AS k, group_array(number), groupBitOr(number), groupBitAnd(number), groupBitXor(number) FROM (SELECT * FROM system.numbers LIMIT 20) GROUP BY k ORDER BY k;
