/* Aggregate function 'uniq' is intended to be associative and provide deterministic results regardless to the schedule of query execution threads and remote servers in a cluster.
 * But due to subtle bug in implementation it is not associative in very rare cases.
 * In this test we fill data structure with specific pattern that reproduces this behaviour.
 */
set query_mode='table';
set asterisk_include_reserved_columns=false;

DROP TABLE IF EXISTS part_a;
DROP TABLE IF EXISTS part_b;
DROP TABLE IF EXISTS part_c;
DROP TABLE IF EXISTS part_d;

/* Create values that will resize hash table to the maximum (131072 cells) and fill it with less than max_fill (65536 cells)
 * and occupy cells near the end except last 10 cells:
 * [               -----------  ]
 * Pick values that will vanish if table will be rehashed.
 */
create stream part_a Engine = MergeTree ORDER BY place AS (SELECT * FROM
(
WITH
    number AS k1,
    bit_xor(k1, bit_shift_right(k1, 33)) AS k2,
    k2 * 0xff51afd7ed558ccd AS k3,
    bit_xor(k3, bit_shift_right(k3, 33)) AS k4,
    k4 * 0xc4ceb9fe1a85ec53 AS k5,
    bit_xor(k5, bit_shift_right(k5, 33)) AS k6,
    k6 AS hash,
    bit_shift_right(hash, 15) % 0x20000 AS place,
    hash % 2 = 0 AS will_remain
SELECT hash, number, place FROM system.numbers WHERE place >= 90000 AND place < 131062 AND NOT will_remain LIMIT 1 BY place LIMIT 41062
));

/* Create values that will resize hash table to the maximum (131072 cells) and fill it with less than max_fill (65536 cells),
 * but if we use both "a" and "b", it will force rehash.
 * [      -----------           ]
 * Pick values that will remain after rehash.
 */
<<<<<<< HEAD:tests/queries/0_stateless/00973_uniq_non_associativity.sql
CREATE TABLE part_b ENGINE = TinyLog AS SELECT * FROM
=======
create stream part_b  Engine = MergeTree ORDER BY place AS SELECT * FROM
>>>>>>> 9e73b005c8... CH porting case ,v3:tests/queries_ported/0_stateless/00973_uniq_non_associativity.sql
(
WITH
    number AS k1,
    bit_xor(k1, bit_shift_right(k1, 33)) AS k2,
    k2 * 0xff51afd7ed558ccd AS k3,
    bit_xor(k3, bit_shift_right(k3, 33)) AS k4,
    k4 * 0xc4ceb9fe1a85ec53 AS k5,
    bit_xor(k5, bit_shift_right(k5, 33)) AS k6,
    k6 AS hash,
    bit_shift_right(hash, 15) % 0x20000 AS place,
    hash % 2 = 0 AS will_remain
SELECT hash, number, place FROM system.numbers WHERE place >= 50000 AND place < 90000 AND will_remain LIMIT 1 BY place LIMIT 40000
);

/* Occupy 10 cells near the end of "a":
 * a:     [               -----------  ]
 * c:     [                        --  ]
 * If we insert "a" then "c", these values will be placed at the end of hash table due to collision resolution:
 * a + c: [               aaaaaaaaaaacc]
 */
<<<<<<< HEAD:tests/queries/0_stateless/00973_uniq_non_associativity.sql
CREATE TABLE part_c ENGINE = TinyLog AS SELECT * FROM
=======
create stream part_c Engine = MergeTree ORDER BY place AS SELECT * FROM
>>>>>>> 9e73b005c8... CH porting case ,v3:tests/queries_ported/0_stateless/00973_uniq_non_associativity.sql
(
WITH
    number AS k1,
    bit_xor(k1, bit_shift_right(k1, 33)) AS k2,
    k2 * 0xff51afd7ed558ccd AS k3,
    bit_xor(k3, bit_shift_right(k3, 33)) AS k4,
    k4 * 0xc4ceb9fe1a85ec53 AS k5,
    bit_xor(k5, bit_shift_right(k5, 33)) AS k6,
    k6 AS hash,
    bit_shift_right(hash, 15) % 0x20000 AS place,
    hash % 2 = 0 AS will_remain
SELECT hash, number, place FROM system.numbers WHERE place >= 131052 AND place < 131062 AND will_remain AND hash NOT IN (SELECT hash FROM part_a) LIMIT 1 BY place LIMIT 10
);

/* Occupy 10 cells at the end of hash table, after "a":
 * a:     [               -----------  ]
 * d:     [                          --]
 * a + d: [               aaaaaaaaaaadd]
 * But if we insert "a" then "c" then "d", these values will be placed at the beginning of the hash table due to collision resolution:
 * a+c+d: [dd             aaaaaaaaaaacc]
  */
<<<<<<< HEAD:tests/queries/0_stateless/00973_uniq_non_associativity.sql
CREATE TABLE part_d ENGINE = TinyLog AS SELECT * FROM
=======
create stream part_d Engine = MergeTree ORDER BY place AS SELECT * FROM
>>>>>>> 9e73b005c8... CH porting case ,v3:tests/queries_ported/0_stateless/00973_uniq_non_associativity.sql
(
WITH
    number AS k1,
    bit_xor(k1, bit_shift_right(k1, 33)) AS k2,
    k2 * 0xff51afd7ed558ccd AS k3,
    bit_xor(k3, bit_shift_right(k3, 33)) AS k4,
    k4 * 0xc4ceb9fe1a85ec53 AS k5,
    bit_xor(k5, bit_shift_right(k5, 33)) AS k6,
    k6 AS hash,
    bit_shift_right(hash, 15) % 0x20000 AS place,
    hash % 2 = 0 AS will_remain
SELECT hash, number, place FROM system.numbers WHERE place >= 131062 AND will_remain LIMIT 1 BY place LIMIT 10
);

/** What happens if we insert a then c then d then b?
  * Insertion of b forces rehash.
  * a will be removed, but c, d, b remain:
  * [dd         bbbbbbbbbb     cc]
  * Then we go through hash table and move elements to better places in collision resolution chain.
  * c will be moved left to their right place:
  * [dd         bbbbbbbbbb   cc  ]
  *
  * And d must be moved also:
  * [           bbbbbbbbbb   ccdd]
  * But our algorithm was incorrect and it doesn't happen.
  *
  * If we insert d again, it will be placed twice because original d will not found:
  * [dd         bbbbbbbbbb   ccdd]
  * This will lead to slightly higher return value of "uniq" aggregate function and it is dependent on insertion order.
  */


SET max_threads = 1;

/** Results of these two queries must match: */

SELECT uniq(number) FROM (
          SELECT * FROM part_a
UNION ALL SELECT * FROM part_c
UNION ALL SELECT * FROM part_d
UNION ALL SELECT * FROM part_b);

SELECT uniq(number) FROM (
          SELECT * FROM part_a
UNION ALL SELECT * FROM part_c
UNION ALL SELECT * FROM part_d
UNION ALL SELECT * FROM part_b
UNION ALL SELECT * FROM part_d);


DROP TABLE part_a;
DROP TABLE part_b;
DROP TABLE part_c;
DROP TABLE part_d;
