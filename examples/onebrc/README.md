# The One Billion Row Challenge with Proton

Back in January, Gunnar Morling kicked off a challenge to optimise the aggregation of a billion rows nicknamed the `1brc` ([One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/)):

> “Your mission, should you decide to accept it, is deceptively simple: write a Java program for retrieving temperature measurement values from a text file and calculating the min, mean, and max temperature per weather station. There’s just one caveat: the file has 1,000,000,000 rows!”

The constraints of the [`1brc`](https://github.com/gunnarmorling/1brc) were carefully selected to raise awareness, within the Java community, of new language features that many professional Java developers are not aware of. But, the optimisation task at the core of challenge turned out to be sufficiently challenging on its own, making it hugely popular amongst developers of all stripes.

## Language Shootout
There was plenty of interest from other language communities. It wasn't long before the challenge turned into a language shootout: to see which language could produce the fastest solution, even though only solutions written in Java would be accepted for judging.

### Programming Languages
Highly optimized solutions for the challenge were written in a wide variety of programming languages including C, C++, C#, Dart, Elixir, Erlang, Go, Haskell, JavaScript, Kotlin, Lua, Perl, Python, R, Ruby, Rust, Scala, Swift, Zig and even less popular programming languages like COBOL, Crystal and Fortran. 

### Query Languages
Query languages were not left out. Solutions were shared in multiple SQL dialects, including:
* ClickHouse SQL[^a][^2];
* Databend Cloud SQL[^7];
* DuckDB SQL[^1];
* MySQL SQL[^6];
* Oracle SQL[^5a][^5b];
* Postgres SQL[^2];
* Snowflake SQL[^4a][^4b];
* TinyBird SQL[^3].

There was also an attempt written in KDB/Q[^8]—an SQL-like, general-purpose programming language built on top of KDB+. This isn't surprising since query languages shine really well in data aggregation tasks. 

This article will share a solution for the challenge in the Proton SQL dialect.

## Proton
Proton is a purpose-built streaming analytics engine that comes bundled with [two data stores](https://docs.timeplus.com/proton-architecture#data-storage): 
- Timeplus NativeLog data store for real-time streaming queries and;
- ClickHouse data store for historical queries.

Since the input data for the `1brc` is a static 13GB CSV file and not a streaming data source, we will simply adapt the solution written in the ClickHouse SQL dialect so it can work inside Proton for this demo. 

### Adapting the ClickHouse SQL for Proton
The core of the ClickHouse SQL solution[^a] can be seen below:
```sql
SET format_csv_delimiter = ';';

SELECT 
    concat('{', arrayStringConcat(groupArray(formatted_result), ', '), '}') AS final_output
FROM (
    SELECT 
        format('{}={}/{}/{}', city, toDecimalString(min(temperature), 1), toDecimalString(avg(temperature), 1), toDecimalString(max(temperature), 1)) AS formatted_result
    FROM file('measurements.txt', 'CSV', 'city String, temperature Float32')
    GROUP BY city
    ORDER BY city
)
```

The Proton docs covers the [minor differences](https://docs.timeplus.com/proton-faq/#if-im-familiar-with-clickhouse-how-easy-is-it-for-me-to-use-proton) between the ClickHouse SQL dialect and the Proton SQL dialect. 

In all, the changes were minimal to make the SQL work on Proton:
* change `arrayStringConcat()` => `array_string_concat()`;
* change `groupArray()` => `groupArray()`;
* change `toDecimalString()` => `to_string(to_decimal(...))`;
* change `city String` => `city string`;
* change `temperature Float32` => `temperature float32`.

The SQL code that we will be using on Proton:
```sql
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
```



## Footnotes
[^a]: [1BRC in ClickHouse SQL #80](https://github.com/gunnarmorling/1brc/discussions/80)
[^1]: [1BRC in SQL with DuckDB #39](https://github.com/gunnarmorling/1brc/discussions/39) or *[1️⃣🐝🏎️🦆 (1BRC in SQL with DuckDB)](https://rmoff.net/2024/01/03/1%EF%B8%8F%E2%83%A3%EF%B8%8F-1brc-in-sql-with-duckdb/)*
[^2]: [1BRC with PostgreSQL and ClickHouse #81](https://github.com/gunnarmorling/1brc/discussions/81) or *[1 billion rows challenge in PostgreSQL and ClickHouse](https://ftisiot.net/posts/1brows/)*
[^3]: [1BRC in Tinybird #244](https://github.com/gunnarmorling/1brc/discussions/244) 
[^4a]: [The One Billion Row Challenge with Snowflake](https://medium.com/snowflake/the-one-billion-row-challenge-with-snowflake-f612ae76dbd5)
[^4b]: [1BRC in SQL with Snowflake #188](https://github.com/gunnarmorling/1brc/discussions/188)
[^5a]: [1 billion row challenge in SQL and Oracle Database](https://geraldonit.com/2024/01/31/1-billion-row-challenge-in-sql-and-oracle-database/)
[^5b]: [1BRC with Oracle Database #707](https://github.com/gunnarmorling/1brc/discussions/707)
[^6]: [1BRC with MySQL #594](https://github.com/gunnarmorling/1brc/discussions/594)
[^7]: [1BRC in SQL with Databend Cloud #230](https://github.com/gunnarmorling/1brc/discussions/230)
[^8]: [1BRC in KDB/Q #208](https://github.com/gunnarmorling/1brc/discussions/208) 
