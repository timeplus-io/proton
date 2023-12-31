SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test;
create stream test(start Integer, end Integer) engine = Memory;
INSERT INTO test(start,end) VALUES (1,3),(2,7),(3,999),(4,7),(5,8);
SELECT sleep(3);

/*
1 2 3 4 5 6 7 8 9
------------------>
1---3
  2---------7
    3-------------
      4-----7
        5-----8
------------------>
1 2 3 3 4 4 4 2 1  //intersections count for each point
*/

SELECT maxIntersections(start,end) FROM test;
SELECT maxIntersectionsPosition(start,end) FROM test;

DROP STREAM test;
