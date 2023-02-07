-- { echoOn }

select sum_if(1, NULL);
select sum_if(NULL, 1);
select sum_if(NULL, NULL);
select countIf(1, NULL);
select countIf(NULL, 1);
select countIf(1, NULL);
select sumArray([NULL, NULL]);
select countArray([NULL, NULL]);

