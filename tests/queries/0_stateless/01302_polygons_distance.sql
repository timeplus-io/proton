select polygonsDistanceCartesian([[[(0, 0),(0, 3),(1, 2.9),(2, 2.6),(2.6, 2),(2.9, 1),(3, 0),(0, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]);
select polygonsDistanceCartesian([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(1., 1.),(1., 4.),(4., 4.),(4., 1.),(1., 1.)]]]);
select polygonsDistanceSpherical([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]);

SET query_mode = 'table';
drop stream if exists polygon_01302;
create stream polygon_01302 (x array(array(array(tuple(float64, float64)))), y array(array(array(tuple(float64, float64))))) engine=Memory();
insert into polygon_01302 values ([[[(23.725750, 37.971536)]]], [[[(4.3826169, 50.8119483)]]]);
select polygonsDistanceSpherical(x, y) from polygon_01302;

drop stream polygon_01302;
