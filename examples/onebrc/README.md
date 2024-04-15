# The One Billion Row Challenge with Timeplus Proton

Back in January, Gunnar Morling kicked off a challenge to optimise the aggregation of a billion rows nicknamed the `1brc` ([One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/)):

> “Your mission, should you decide to accept it, is deceptively simple: write a Java program for retrieving temperature measurement values from a text file and calculating the min, mean, and max temperature per weather station. There’s just one caveat: the file has 1,000,000,000 rows!”

The constraints of the [`1brc`](https://github.com/gunnarmorling/1brc) were carefully selected to raise awareness, within the Java community, of new language features that many professional Java developers are not aware of. But, the optimisation task at the core of challenge turned out to be sufficiently challenging on its own, making it hugely popular amongst developers of all stripes.

## Language Shootout
There was plenty of interest from other language communities. It wasn't long before the challenge turned into a language shootout: to see which language could produce the fastest solution, even though only solutions written in Java would be accepted for judging.

### Programming Languages
Highly optimized solutions for the challenge were written in a wide variety of programming languages including C, C++, C#, Dart, Elixir, Erlang, Go, Haskell, JavaScript, Kotlin, Lua, Perl, PHP, Python, R, Ruby, Rust, Scala, Swift, Zig and even less popular programming languages like COBOL, Crystal and Fortran. 

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

This article will share a solution for the challenge in the Timeplus SQL dialect.

## Timeplus Proton
Timeplus Proton is a purpose-built streaming analytics engine that comes bundled with [two data stores](https://docs.timeplus.com/proton-architecture#data-storage): 
- Timeplus NativeLog data store for real-time streaming queries and;
- A historical data store, powered by ClickHouse, for fast batch queries.

Since the input data for the `1brc` is a static 13GB CSV file and not a streaming data source, we will simply adapt the solution written in the ClickHouse SQL dialect so it can work inside Timeplus Proton for this demo. 

### Adapting the ClickHouse SQL for Timeplus Proton
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

The Timeplus Proton docs covers the [minor differences](https://docs.timeplus.com/proton-faq/#if-im-familiar-with-clickhouse-how-easy-is-it-for-me-to-use-proton) between the ClickHouse SQL dialect and the Timeplus SQL dialect. 

In all, the changes were minimal to make the SQL work on Timeplus Proton:
* convert column types from title case to lower case:
  * in `city String`, change `String` => `string`;
  * in `temperature Float32`, change `Float32` => `float32`;
* convert function names from camel case to snake case:
  * change `arrayStringConcat()` => `array_string_concat()`;
  * change `groupArray()` => `group_array()`;
* in the absence of a single function, compose multiple functions to obtain the same result: 
  * change `toDecimalString()` => `to_string(to_decimal(...))`.

The SQL code that we will be using on Timeplus Proton:
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
1. Make a separate folder for this demo to make it easy to clean up after we are done:
```bash
sudo mkdir -p /demo
sudo chown -R ubuntu:ubuntu /demo
```

2. The official test data generator was written for Java 21. We will first install [`sdkman`](https://sdkman.io/jdks) to allow us manage multiple Java versions:
```bash
curl -s "https://get.sdkman.io" | bash
source "/home/ubuntu/.sdkman/bin/sdkman-init.sh"
```

3. Using `sdkman`, install a JDK based on OpenJDK that supports Java 21:
```bash
sdk install java 21.0.2-open
```

4. Clone the [`1brc`](https://github.com/gunnarmorling/1brc) repository locally:
```bash
cd /demo
git clone https://github.com/gunnarmorling/1brc
```

5. Build the generator for the test data 
```bash
cd /demo/1brc
./mvnw verify 
```

6. Use the generator to create 1 billion rows of test data
```bash
time ./create_measurements.sh 1000000000
```

<details>
<summary>The generator generated <code>measurements.txt</code> (a 13GB CSV file) in about 10 minutes.</summary>
<pre>
time ./create_measurements.sh 1000000000
Wrote 50,000,000 measurements in 15007 ms
Wrote 100,000,000 measurements in 46490 ms
Wrote 150,000,000 measurements in 77952 ms
Wrote 200,000,000 measurements in 109410 ms
Wrote 250,000,000 measurements in 140847 ms
Wrote 300,000,000 measurements in 172254 ms
Wrote 350,000,000 measurements in 203742 ms
Wrote 400,000,000 measurements in 235155 ms
Wrote 450,000,000 measurements in 266711 ms
Wrote 500,000,000 measurements in 298319 ms
Wrote 550,000,000 measurements in 329901 ms
Wrote 600,000,000 measurements in 361338 ms
Wrote 650,000,000 measurements in 392873 ms
Wrote 700,000,000 measurements in 424411 ms
Wrote 750,000,000 measurements in 455838 ms
Wrote 800,000,000 measurements in 487368 ms
Wrote 850,000,000 measurements in 518836 ms
Wrote 900,000,000 measurements in 550312 ms
Wrote 950,000,000 measurements in 582063 ms
Created file with 1,000,000,000 measurements in 613590 ms

real    10m13.660s
user    9m54.950s
sys     0m18.579s

du -shL measurements.txt 
13G     measurements.txt
</pre>
</details>


## `1brc` Demo using Timeplus Proton
1. Install Timeplus Proton on your machine:
```bash
cd /demo
curl https://install.timeplus.com | sh
```

2. Make sure your current working directory remains `demo`. Start the Timeplus Proton server in this directory:
```bash
./proton server
```

3. Once the Timeplus Proton server is started successfully, it will create a folder named `proton-data/` in the current directory which contains multiple subfolders. We will create a symbolic link to the test data we generated earlier (`measurements.txt`) from the `proton-data/user_files` subfolder created by the Timeplus Proton server.
```bash
ln -s /demo/1brc/measurements.txt /demo/proton-data/user_files
```

4. Now download the [query](1brc.sql) we will be executing against the Timeplus Proton server to a file named `1brc.sql` on your machine.

5. Next, from another terminal, start the Timeplus Proton client with the `1brc.sql` file as input:
```bash
time ./proton client --host 127.0.0.1 --multiquery < /demo/1brc.sql
```

<details>
<summary>Aggregation of 1 billion rows in Timeplus Proton took just <code>39.301 seconds</code> (versus <code>47.053 seconds</code> for ClickHouse) on my machine.</summary>
<pre>
{Abha=-32.9/17.9/66.1, Abidjan=-23.8/25.9/75.9, Abéché=-18.7/29.3/77, Accra=-29.2/26.3/77.8, Addis Ababa=-31/16/66.1, Adelaide=-32.8/17.2/66, Aden=-17.7/29/76.5, Ahvaz=-24.2/25.4/80.6, Albuquerque=-33.5/13.9/64.6, Alexandra=-35.7/11/61.5, Alexandria=-29.6/20/73.2, Algiers=-39.5/18.2/66.9, Alice Springs=-30.7/20.9/70.1, Almaty=-39.8/9.9/57.9, Amsterdam=-41.3/10.2/60.4, Anadyr=-57.3/-6.8/47.7, Anchorage=-54.7/2.8/51.6, Andorra la Vella=-39.1/9.8/61.5, Ankara=-39.1/12/57.7, Antananarivo=-32.3/17.8/69.3, Antsiranana=-24.2/25.2/77.7, Arkhangelsk=-50.6/1.2/48.6, Ashgabat=-36.1/17/65.2, Asmara=-38/15.6/63.6, Assab=-19.6/30.4/79.8, Astana=-45.2/3.5/52.6, Athens=-28.2/19.2/68.4, Atlanta=-32.9/16.9/68.8, Auckland=-32/15.2/66.8, Austin=-28.1/20.6/73.5, Baghdad=-28.4/22.7/72.5, Baguio=-40.8/19.5/67.9, Baku=-32.8/15/63.7, Baltimore=-37.8/13.1/59.7, Bamako=-22.9/27.7/83, Bangkok=-21.9/28.6/78.1, Bangui=-28.6/25.9/79.4, Banjul=-25.6/25.9/77.2, Barcelona=-29.7/18.2/68.6, Bata=-25.4/25.1/73.4, Batumi=-35.1/14/61.6, Beijing=-34.8/12.9/63.1, Beirut=-30.4/20.9/78, Belgrade=-35.9/12.4/63.8, Belize City=-21.8/26.7/78.4, Benghazi=-30.1/19.9/67.2, Bergen=-39.6/7.7/58.2, Berlin=-40.5/10.2/59.6, Bilbao=-34.4/14.7/67.3, Birao=-24.2/26.5/77.5, Bishkek=-37.5/11.3/62.8, Bissau=-23.1/27/78.9, Blantyre=-28.5/22.2/71.6, Bloemfontein=-34.9/15.5/64.8, Boise=-42.3/11.3/61, Bordeaux=-37.5/14.1/66.9, Bosaso=-18.9/30/81.6, Boston=-41.2/10.9/58.6, Bouaké=-23.7/25.9/76.7, Bratislava=-40.8/10.5/61.9, Brazzaville=-25.5/24.9/77.3, Bridgetown=-24.2/27/78.8, Brisbane=-25.3/21.3/72.7, Brussels=-41.4/10.4/60.8, Bucharest=-44.9/10.8/61.3, Budapest=-42.1/11.3/60.6, Bujumbura=-24.9/23.7/72.1, Bulawayo=-37.4/18.9/68.6, Burnie=-35.8/13/61.9, Busan=-33.6/15/69.8, Cabo San Lucas=-26.8/23.8/72.9, Cairns=-22/24.9/75.7, Cairo=-29.2/21.4/73.5, Calgary=-46.6/4.3/52.7, Canberra=-36.4/13.1/64.2, Cape Town=-32.3/16.2/63.8, Changsha=-32/17.3/69.9, Charlotte=-36.1/16/68.8, Chiang Mai=-23.9/25.7/78, Chicago=-40.4/9.7/59.7, Chihuahua=-28.4/18.6/74.2, Chittagong=-27.1/25.8/76.4, Chișinău=-39.3/10.2/58.5, Chongqing=-35.6/18.6/73.3, Christchurch=-37/12.1/60.7, City of San Marino=-37.2/11.7/63.6, Colombo=-20.8/27.3/76.4, Columbus=-38.9/11.6/61.4, Conakry=-28.4/26.3/75.1, Copenhagen=-42.8/9/65.3, Cotonou=-26.5/27.1/80, Cracow=-40.8/9.2/61.3, Da Lat=-30.3/17.9/66.4, Da Nang=-22/25.8/75.6, Dakar=-27.7/23.9/75, Dallas=-33.3/19/71.4, Damascus=-37.1/17/68.4, Dampier=-29.3/26.4/78.4, Dar es Salaam=-26.6/25.8/77.9, Darwin=-21.4/27.5/74.6, Denpasar=-28.9/23.6/73.8, Denver=-39.8/10.3/56, Detroit=-40.8/9.9/60, Dhaka=-21.2/25.9/74.8, Dikson=-60.5/-11.1/39.7, Dili=-21.4/26.6/74.8, Djibouti=-21.1/29.9/81.6, Dodoma=-27.8/22.7/69.4, Dolisie=-27.5/24/71.5, Douala=-22/26.7/78.8, Dubai=-21.2/26.8/79.8, Dublin=-36.6/9.8/59.6, Dunedin=-39/11/59, Durban=-32.5/20.6/72.4, Dushanbe=-34.2/14.7/69, Edinburgh=-43.3/9.2/58.2, Edmonton=-44.6/4.1/54.8, El Paso=-31.9/18.1/66.1, Entebbe=-28.9/21/70.9, Erbil=-35.4/19.4/78.7, Erzurum=-44.4/5/53.8, Fairbanks=-50.7/-2.2/46.9, Fianarantsoa=-30.2/17.8/68, Flores,  Petén=-24.9/26.3/77.5, Frankfurt=-37.7/10.6/60.9, Fresno=-35.2/17.8/67.1, Fukuoka=-29.9/17/70.3, Gaborone=-30.4/20.9/70.8, Gabès=-28.6/19.4/69, Gagnoa=-22.4/25.9/76.3, Gangtok=-34.9/15.2/64.4, Garissa=-17.8/29.3/77.3, Garoua=-17.3/28.2/76.9, George Town=-21.9/27.8/75.5, Ghanzi=-30.5/21.3/76.6, Gjoa Haven=-64.9/-14.3/38, Guadalajara=-27.4/20.9/70.7, Guangzhou=-27.1/22.4/71, Guatemala City=-35.6/20.4/70.7, Halifax=-42.9/7.5/57.2, Hamburg=-39.6/9.6/63.6, Hamilton=-40/13.8/62.5, Hanga Roa=-32/20.5/75.6, Hanoi=-26.8/23.6/75.1, Harare=-31.1/18.4/66.9, Harbin=-42.9/5/56.9, Hargeisa=-26.5/21.6/70.9, Hat Yai=-23.6/26.9/75.1, Havana=-29/25.2/73.3, Helsinki=-43.6/5.8/53.5, Heraklion=-32.3/18.9/69.8, Hiroshima=-32/16.3/66, Ho Chi Minh City=-22.1/27.4/82.4, Hobart=-45.2/12.7/66.5, Hong Kong=-25.9/23.2/74.9, Honiara=-24.4/26.5/74.9, Honolulu=-24.6/25.4/78.3, Houston=-30.3/20.8/69, Ifrane=-39.3/11.3/60.1, Indianapolis=-40/11.8/67.6, Iqaluit=-58.1/-9.3/40.7, Irkutsk=-48.8/0.9/56.4, Istanbul=-37.9/13.9/67.8, Jacksonville=-31.5/20.2/70, Jakarta=-22.3/26.6/78.4, Jayapura=-20.7/27/75.7, Jerusalem=-32.2/18.2/69.2, Johannesburg=-33/15.4/67.4, Jos=-35.8/22.8/72, Juba=-23/27.7/75.1, Kabul=-37.2/12.1/59.2, Kampala=-31.3/19.9/69.6, Kandi=-25.9/27.6/79.5, Kankan=-23/26.4/75.7, Kano=-24.8/26.4/79.1, Kansas City=-35/12.4/61.5, Karachi=-29.1/25.9/75.1, Karonga=-24.4/24.4/74.2, Kathmandu=-31.7/18.2/66, Khartoum=-20.8/29.9/84.5, Kingston=-20.7/27.4/74.6, Kinshasa=-25.6/25.2/76.5, Kolkata=-21.2/26.6/74.8, Kuala Lumpur=-23.4/27.2/80.4, Kumasi=-26.1/25.9/77.5, Kunming=-35.7/15.6/65.8, Kuopio=-47.6/3.3/54.1, Kuwait City=-27.3/25.7/74.7, Kyiv=-45.7/8.4/58, Kyoto=-35.4/15.8/74.2, La Ceiba=-21.2/26.2/78.7, La Paz=-25.4/23.6/76.5, Lagos=-21.3/26.8/75.2, Lahore=-29.1/24.2/74.3, Lake Havasu City=-26.3/23.6/71.6, Lake Tekapo=-47.3/8.6/56.3, Las Palmas de Gran Canaria=-31.3/21.1/70.2, Las Vegas=-31/20.2/72.7, Launceston=-35.1/13.1/62.8, Lhasa=-41.4/7.5/60.4, Libreville=-23.9/25.8/74.4, Lisbon=-36.5/17.5/69.6, Livingstone=-31.6/21.7/77.2, Ljubljana=-37.5/10.9/59.7, Lodwar=-19.6/29.3/80.6, Lomé=-22.8/26.9/79.7, London=-40.5/11.3/64.5, Los Angeles=-36.6/18.5/69.8, Louisville=-36.2/13.9/63.2, Luanda=-22.8/25.8/74.7, Lubumbashi=-31.3/20.7/69, Lusaka=-32.2/19.8/67.4, Luxembourg City=-39.6/9.3/58.4, Lviv=-40.9/7.7/55.9, Lyon=-35.7/12.5/61, Madrid=-35.6/14.9/71.1, Mahajanga=-20.5/26.2/78.6, Makassar=-21.8/26.7/77.6, Makurdi=-24.8/26/76.6, Malabo=-25.2/26.3/78.2, Malé=-21.6/28/80.8, Managua=-23.9/27.2/76.6, Manama=-24/26.5/76.5, Mandalay=-21/28/78.7, Mango=-20.1/28.1/82.1, Manila=-20.9/28.4/78.5, Maputo=-27.4/22.8/71.3, Marrakesh=-33.4/19.5/69.2, Marseille=-33.7/15.8/64.9, Maun=-28.9/22.4/77.1, Medan=-22.9/26.4/76.8, Mek\'ele=-27.2/22.7/72.6, Melbourne=-35.6/15/64, Memphis=-34.3/17.2/67.4, Mexicali=-26.3/23/71, Mexico City=-32.5/17.4/65.7, Miami=-32.4/24.8/76, Milan=-34.9/12.9/67.9, Milwaukee=-41.2/8.9/62.7, Minneapolis=-43.9/7.7/58.6, Minsk=-45.3/6.6/55.4, Mogadishu=-25.4/27/74.6, Mombasa=-27.2/26.3/75.3, Monaco=-31/16.3/66.6, Moncton=-42/6.1/58, Monterrey=-28.1/22.3/71.7, Montreal=-41.1/6.7/57.3, Moscow=-45.4/5.7/54.3, Mumbai=-19.9/27.1/76, Murmansk=-47/0.6/49.4, Muscat=-25.8/27.9/77.8, Mzuzu=-31.7/17.6/66.5, N\'Djamena=-18.4/28.2/81.4, Naha=-25.7/23.1/74.6, Nairobi=-38.8/17.7/68, Nakhon Ratchasima=-23.9/27.2/76.2, Napier=-35.5/14.5/63.5, Napoli=-32.9/15.8/70.9, Nashville=-34.4/15.4/65.1, Nassau=-29/24.6/72.9, Ndola=-30/20.3/68.3, New Delhi=-27.7/24.9/80.8, New Orleans=-27.8/20.6/71.5, New York City=-37.1/12.8/63.1, Ngaoundéré=-27.6/21.9/77.4, Niamey=-20.1/29.3/76, Nicosia=-28.9/19.7/68.2, Niigata=-34/13.8/63.1, Nouadhibou=-28.6/21.2/74.6, Nouakchott=-22.9/25.7/73.6, Novosibirsk=-46.9/1.7/50.4, Nuuk=-50.8/-1.4/48.2, Odesa=-36.5/10.7/60.5, Odienné=-24.7/25.9/74.4, Oklahoma City=-36/15.8/66.2, Omaha=-40.9/10.6/59.8, Oranjestad=-21.8/28.1/81.3, Oslo=-45.8/5.6/56.2, Ottawa=-44.7/6.5/56.7, Ouagadougou=-24.7/28.3/78.1, Ouahigouya=-21.7/28.5/87, Ouarzazate=-30.5/18.8/69.9, Oulu=-47.8/2.6/50.4, Palembang=-26/27.2/76.4, Palermo=-41.1/18.5/75.6, Palm Springs=-24.7/24.5/72, Palmerston North=-38.4/13.1/66.4, Panama City=-21.7/27.9/75.9, Parakou=-27.4/26.7/80.4, Paris=-34.3/12.2/65.7, Perth=-31.6/18.7/67, Petropavlovsk-Kamchatsky=-48.8/1.8/49, Philadelphia=-34.9/13.2/61.7, Phnom Penh=-22.2/28.2/75.1, Phoenix=-22.5/23.9/74.1, Pittsburgh=-38/10.7/62.7, Podgorica=-39.9/15.2/66.8, Pointe-Noire=-22.9/26/74.2, Pontianak=-20.6/27.7/81, Port Moresby=-24.2/26.9/75.7, Port Sudan=-18.7/28.3/77.7, Port Vila=-25.8/24.3/72.5, Port-Gentil=-24.3/25.9/81.2, Portland (OR)=-34.9/12.4/63.9, Porto=-30.9/15.7/63, Prague=-42.3/8.3/63.7, Praia=-23.3/24.4/73.1, Pretoria=-31.1/18.1/67.3, Pyongyang=-41.2/10.7/62.6, Rabat=-33.3/17.2/63.5, Rangpur=-25.6/24.4/76.1, Reggane=-19.9/28.3/79.9, Reykjavík=-43/4.2/53, Riga=-42.7/6.2/55, Riyadh=-24.1/26/74, Rome=-37/15.2/64.5, Roseau=-24.5/26.2/75.7, Rostov-on-Don=-40.9/9.9/59.5, Sacramento=-32.1/16.3/67.5, Saint Petersburg=-50.7/5.7/55.5, Saint-Pierre=-46.1/5.7/58.1, Salt Lake City=-39.7/11.5/64.9, San Antonio=-26.4/20.7/69.5, San Diego=-34.5/17.8/67.2, San Francisco=-36.1/14.5/65.4, San Jose=-32.4/16.4/76, San José=-27.9/22.5/79, San Juan=-26.7/27.2/77.6, San Salvador=-26.9/23/78.6, Sana\'a=-29.4/19.9/65.5, Santo Domingo=-23.1/25.8/77.7, Sapporo=-42.9/8.9/62.8, Sarajevo=-42.4/10/61.6, Saskatoon=-48.9/3.3/51.6, Seattle=-37.1/11.2/58.8, Seoul=-36.7/12.4/61.7, Seville=-33.1/19.2/72.6, Shanghai=-34.7/16.6/68.5, Singapore=-22.7/27/78.7, Skopje=-36.4/12.3/69.5, Sochi=-34.1/14.1/62.8, Sofia=-41.3/10.6/66.4, Sokoto=-21.6/27.9/78.6, Split=-34/16.1/64.2, St. John\'s=-42.7/5/52.5, St. Louis=-34.9/13.8/62, Stockholm=-41.6/6.6/56.2, Surabaya=-23.9/27.1/74.9, Suva=-27.5/25.5/72.3, Suwałki=-45.1/7.1/57.8, Sydney=-31/17.7/71.1, Ségou=-24.3/28/76.5, Tabora=-24.5/22.9/71, Tabriz=-37.6/12.6/62.6, Taipei=-25.9/22.9/72.9, Tallinn=-43.5/6.3/55.7, Tamale=-19.4/27.8/77.5, Tamanrasset=-29/21.7/70.5, Tampa=-30/22.9/75.3, Tashkent=-33.2/14.7/70, Tauranga=-34.9/14.8/64.4, Tbilisi=-35.4/12.9/65.2, Tegucigalpa=-27.6/21.6/71.2, Tehran=-32.8/16.9/68.1, Tel Aviv=-30.6/20/70.8, Thessaloniki=-32.8/16/66.7, Thiès=-23.1/23.9/77, Tijuana=-30.5/17.8/68.3, Timbuktu=-19.1/28/77.8, Tirana=-34.4/15.2/65.5, Toamasina=-24.7/23.4/81.3, Tokyo=-37.5/15.3/65.7, Toliara=-25/24/75.2, Toluca=-43.8/12.3/61.5, Toronto=-45.3/9.3/62.7, Tripoli=-32.8/20/70.3, Tromsø=-50.2/2.9/52.7, Tucson=-29.9/20.8/68.7, Tunis=-31.8/18.4/72.2, Ulaanbaatar=-55.7/-0.4/50, Upington=-29.2/20.4/67.9, Vaduz=-39.9/10/62.9, Valencia=-33.3/18.2/73.1, Valletta=-29.8/18.7/68.1, Vancouver=-37.8/10.3/65.1, Veracruz=-24.9/25.3/79.9, Vienna=-42.4/10.4/60, Vientiane=-21.7/25.8/79.4, Villahermosa=-26.3/27.1/73.5, Vilnius=-47.1/6/61.8, Virginia Beach=-34.5/15.8/65.1, Vladivostok=-42.8/4.8/54.6, Warsaw=-44.5/8.4/57.2, Washington, D.C.=-33.1/14.6/68.6, Wau=-28.6/27.7/78.5, Wellington=-36.6/12.8/63, Whitehorse=-53.8/-0.1/50, Wichita=-39.5/13.9/63.2, Willemstad=-19.4/27.9/79.5, Winnipeg=-45.3/3/49.5, Wrocław=-40.8/9.6/63.9, Xi\'an=-34.3/14/64.4, Yakutsk=-57.6/-8.8/38.6, Yangon=-22.9/27.5/81.8, Yaoundé=-29.3/23.7/74.7, Yellowknife=-53.2/-4.2/45.3, Yerevan=-38.9/12.4/62, Yinchuan=-40.1/8.9/65.5, Zagreb=-39.5/10.6/62.5, Zanzibar City=-22.6/25.9/76.7, Zürich=-38.4/9.2/56.6, Ürümqi=-44.7/7.3/58.1, İzmir=-30.1/17.8/66.7}

./proton client --host 127.0.0.1 --multiquery < 1brc.sql  0.28s user 0.05s system 0% cpu 39.301 total
</pre>
</details>

The Timeplus Proton server reported a summary of the executed query in its console output:
```bash
2024.04.13 23:07:16.705492 [ 14810 ] {33215e8b-670e-464b-a23e-994f706b7a64} <Information> executeQuery: Read 1000000000 rows, 19.51 GiB in 39.038123 sec., 25615985 rows/sec., 511.89 MiB/sec.
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
