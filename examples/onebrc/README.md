# The One Billion Row Challenge with Proton

Back in January, Gunnar Morling kicked off a challenge to optimise the aggregation of a billion rows nicknamed the `1brc` ([One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/)):

> “Your mission, should you decide to accept it, is deceptively simple: write a Java program for retrieving temperature measurement values from a text file and calculating the min, mean, and max temperature per weather station. There’s just one caveat: the file has 1,000,000,000 rows!”

`1brc` was intended to raise awareness within the Java community about new language features that many Java developers may not be aware of. But, the optimisation task at the core of challenge turned out to be sufficiently challenging on its own, making it hugely popular amongst developers of all stripes.

## Programming Language Shootout
There was plenty of interest from other programming language communities. It wasn't long before the challenge turned into a programming language shootout: to see which programming language could produce the fastest solution, even though only solutions written in Java would be accepted for judging.

Highly optimized solutions for the challenge were written in a wide variety of languages including C, C++, C#, Dart, Elixir, Erlang, Go, Haskell, JavaScript, Kotlin, Lua, Perl, Python, R, Ruby, Rust, Scala, Swift, Zig and even less popular programming languages like COBOL, Crystal and Fortran. 

## Query Languages Shootout
Query languages were not left out. Solutions were shared in multiple SQL dialects. There was also an attempt written in KDB/Q[^8]—an SQL-like, general-purpose programming language built on top of KDB+. This isn't surprising since query languages shine really well in data aggregation tasks. 

Solutions were shared in the multiple SQL dialects, including:
* ClickHouse SQL[^a][^2]
* Databend Cloud SQL[^7]
* DuckDB SQL[^1]
* MySQL SQL[^6]
* Oracle SQL[^5]
* Postgres SQL[^2]
* Snowflake SQL[^4]
* TinyBird SQL[^3]



Robin Moffat shared an SQL-based solution based on DuckDB[^1], while Francesco Tisiot shared another SQL-based solution based on Postgres and ClickHouse[^2].

This article will undertake the challenge using SQL written for the open-source Proton database engine.


## Proton
Because Proton is a purpose-built streaming analytics engine, its actually comes bundled with [two data stores](https://docs.timeplus.com/proton-architecture#data-storage): 
- Timeplus NativeLog data store for real-time streaming queries and;
- ClickHouse data store for historical queries.

Note that the input data for the `1brc` is not a streaming data source. It is a static 13GB CSV file that we will query using the ClickHouse data store. 



[^a]: [1BRC in ClickHouse SQL #80](https://github.com/gunnarmorling/1brc/discussions/80)
[^1]: [1BRC in SQL with DuckDB #39](https://github.com/gunnarmorling/1brc/discussions/39) or *[1️⃣🐝🏎️🦆 (1BRC in SQL with DuckDB)](https://rmoff.net/2024/01/03/1%EF%B8%8F%E2%83%A3%EF%B8%8F-1brc-in-sql-with-duckdb/)*
[^2]: [1BRC with PostgreSQL and ClickHouse #81](https://github.com/gunnarmorling/1brc/discussions/81) or *[1 billion rows challenge in PostgreSQL and ClickHouse](https://ftisiot.net/posts/1brows/)*
[^3]: [1BRC in Tinybird #244](https://github.com/gunnarmorling/1brc/discussions/244) 
[^4]: [1BRC in SQL with Snowflake #188](https://github.com/gunnarmorling/1brc/discussions/188)
[^5]: [1BRC with Oracle Database #707](https://github.com/gunnarmorling/1brc/discussions/707) 
[^6]: [1BRC with MySQL #594](https://github.com/gunnarmorling/1brc/discussions/594)
[^7]: [1BRC in SQL with Databend Cloud #230](https://github.com/gunnarmorling/1brc/discussions/230)
[^8]: [1BRC in KDB/Q #208](https://github.com/gunnarmorling/1brc/discussions/208) 
