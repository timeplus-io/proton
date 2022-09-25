 
SET query_mode = 'table';
drop stream if exists t1_00729;
create stream t1_00729 (id uint64, val array(string),nid uint64, eDate date)ENGINE = MergeTree(eDate, (id, eDate), 8192);

insert into t1_00729 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (2,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (2,[],2,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (3,[],4,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (3,[],5,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (3,[],6,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (3,[],7,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (3,[],8,'2018-09-27');

select array_join(val) as nameGroup6 from t1_00729 prewhere not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }
select array_join(val) as nameGroup6, countDistinct(nid) as rowids from t1_00729 where not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6;
select array_join(val) as nameGroup6, countDistinct(nid) as rowids from t1_00729 prewhere not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }

drop stream t1_00729;
create stream t1_00729 (id uint64, val array(string),nid uint64, eDate date) ENGINE = MergeTree(eDate, (id, eDate), 8192);

insert into t1_00729 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (1,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (2,['background','foreground','heading','image'],1,'2018-09-27');
insert into t1_00729 (id,val,nid,eDate) values (2,[],2,'2018-09-27');

select array_join(val) as nameGroup6 from t1_00729 prewhere not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }
select array_join(val) as nameGroup6, countDistinct(nid) as rowids from t1_00729 where not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6;
select array_join(val) as nameGroup6, countDistinct(nid) as rowids from t1_00729 prewhere not_empty(to_string(nameGroup6)) group by nameGroup6 order by nameGroup6; -- { serverError 182 }

drop stream t1_00729;
