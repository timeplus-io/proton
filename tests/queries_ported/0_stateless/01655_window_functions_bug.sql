SELECT round((count_if(rating = 5)) - (count_if(rating < 5)), 4) as nps,
       dense_rank() OVER (ORDER BY nps DESC)   as rank
FROM (select number as rating, number%3 as rest_id from numbers(10))
group by rest_id
order by rank;
