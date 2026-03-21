select min(sample >= number and sample <= number + 1)
from
(
    select number, rand_uniform(number, number + 1) as sample
    from numbers(20)
);

drop stream if exists 99913_rand_distribution_non_const;

create random stream 99913_rand_distribution_non_const
(
    `base_price` float64 default 100.0 + rand_uniform(0.0, 10.0),
    `spread` float64 default rand_uniform(1.0, 5.0),
    `low_price` float64 default base_price - spread,
    `high_price` float64 default base_price + spread,
    `close_price` float64 default rand_uniform(low_price, high_price)
);

select min(close_price >= low_price and close_price <= high_price)
from
(
    select low_price, high_price, close_price
    from table(99913_rand_distribution_non_const)
    limit 20
);

drop stream if exists 99913_rand_distribution_non_const;
