SET param_num=42;
SET param_str='hello';
SET param_date='2022-08-04 18:30:53';
SET param_map={'2b95a497-3a5d-49af-bf85-15763318cde7': [1.2, 3.4]};
SELECT {num:uint64}, {str:string}, {date:DateTime}, {map:map(uuid, array(float32))};
SELECT to_type_name({num:uint64}), to_type_name({str:string}), to_type_name({date:DateTime}), to_type_name({map:map(uuid, array(float32))});

SET param_id=42;
SET param_arr=[1, 2, 3];
SET param_map_2={'abc': 22, 'def': 33};
SET param_mul_arr=[[4, 5, 6], [7], [8, 9]];
SET param_map_arr={10: [11, 12], 13: [14, 15]};
SET param_map_map_arr={'ghj': {'klm': [16, 17]}, 'nop': {'rst': [18]}};
SELECT {id: int64}, {arr: array(uint8)}, {map_2: map(string, uint8)}, {mul_arr: array(array(uint8))}, {map_arr: map(uint8, array(uint8))}, {map_map_arr: map(string, map(string, array(uint8)))};
SELECT to_type_name({id: int64}), to_type_name({arr: array(uint8)}), to_type_name({map_2: map(string, uint8)}), to_type_name({mul_arr: array(array(uint8))}), to_type_name({map_arr: map(uint8, array(uint8))}), to_type_name({map_map_arr: map(string, map(string, array(uint8)))});

SET param_tbl=numbers;
SET param_db=system;
SET param_col=number;
SELECT {col:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 1 OFFSET 5;

SET param_arr_arr_arr=[[['a', 'b', 'c'], ['d', 'e', 'f']], [['g', 'h', 'i'], ['j', 'k', 'l']]];
SET param_tuple_tuple_tuple=(((1, 'a', '2b95a497-3a5d-49af-bf85-15763318cde7', 3.14)));
SET param_arr_map_tuple=[{1:(2, '2022-08-04 18:30:53', 's'), 3:(4, '2020-08-04 18:30:53', 't')}];
SET param_map_arr_tuple_map={'a':[(1,{10:1, 20:2}),(2, {30:3, 40:4})], 'b':[(3, {50:5, 60:6}),(4, {70:7, 80:8})]};
SELECT {arr_arr_arr: array(array(array(string)))}, to_type_name({arr_arr_arr: array(array(array(string)))});
SELECT {tuple_tuple_tuple: tuple(tuple(tuple(int32, string, uuid, float32)))}, to_type_name({tuple_tuple_tuple: tuple(tuple(tuple(int32, string, uuid, float32)))});
SELECT {arr_map_tuple: array(map(uint64, tuple(int16, DateTime, string)))}, to_type_name({arr_map_tuple: array(map(uint64, tuple(int16, DateTime, string)))});
SELECT {map_arr_tuple_map: map(string, array(tuple(uint8, map(uint32, int64))))}, to_type_name({map_arr_tuple_map: map(string, array(tuple(uint8, map(uint32, int64))))});
