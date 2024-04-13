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
* convert column types from title case to lower case:
  * in `city String`, change `String` => `string`;
  * in `temperature Float32`, change `Float32` => `float32`;
* convert function names from camel case to snake case:
  * change `arrayStringConcat()` => `array_string_concat()`;
  * change `groupArray()` => `group_array()`;
* in the absence of a single function, compose multiple functions to obtain the same result: 
  * change `toDecimalString()` => `to_string(to_decimal(...))`.

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

## Local Setup
### Setup the Test Data
1. The official test data generator was written for Java 21. We will first install [`sdkman`](https://sdkman.io/jdks) to allow us manage multiple Java versions:
```bash
curl -s "https://get.sdkman.io" | bash
source "/home/ubuntu/.sdkman/bin/sdkman-init.sh"
```

2. Install a JDK based on OpenJDK that supports Java 21 using `sdkman`:
```bash
sdk install java 21.0.1-open
```

3. Clone the [`1brc`](https://github.com/gunnarmorling/1brc) repository locally:
```bash
cd /data
git clone https://github.com/gunnarmorling/1brc
```

4. Build the generator for the test data 
```bash
cd 1brc
./mvnw verify 
```

5. Use the generator to create 1 billion rows of test data
```bash
time ./create_measurements.sh 1000000000
```

<details>
<summary>The generator generated `measurements.txt` (a 13GB CSV file) in about 12 minutes.</summary>
<pre>
time ./create_measurements.sh 1000000000
Wrote 50,000,000 measurements in 16122 ms
Wrote 100,000,000 measurements in 55385 ms
Wrote 150,000,000 measurements in 94465 ms
Wrote 200,000,000 measurements in 133557 ms
Wrote 250,000,000 measurements in 172660 ms
Wrote 300,000,000 measurements in 211746 ms
Wrote 350,000,000 measurements in 250862 ms
Wrote 400,000,000 measurements in 290035 ms
Wrote 450,000,000 measurements in 329179 ms
Wrote 500,000,000 measurements in 368350 ms
Wrote 550,000,000 measurements in 407489 ms
Wrote 600,000,000 measurements in 446671 ms
Wrote 650,000,000 measurements in 485740 ms
Wrote 700,000,000 measurements in 524930 ms
Wrote 750,000,000 measurements in 564045 ms
Wrote 800,000,000 measurements in 603177 ms
Wrote 850,000,000 measurements in 642245 ms
Wrote 900,000,000 measurements in 681340 ms
Wrote 950,000,000 measurements in 720459 ms
Created file with 1,000,000,000 measurements in 759563 ms

real    12m39.673s
user    12m24.649s
sys 0m16.623s
</pre>
</details>


## Proton
1. Install Proton on your machine:
```bash
cd /data
curl https://install.timeplus.com | sh
```

2. Start the Proton server:
```bash
./proton server start
```

3. Once the Proton server is started successfully, it will create a folder named `proton-data/` in the current directory which contains multiple subfolders. We will copy the test data we generated earlier (`measurements.txt`) into the `proton-data/user_files` subfolder created by the Proton server.
```bash
cp /data/1brc/measurements.txt /data/proton-data/user_files
```

4. Start the Proton client in another terminal:
```bash
./proton client --host 127.0.0.1
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
