select groupUniqArray(v) from values('id int, v array(int)', (1, [2]), (1, [])) group by id;
