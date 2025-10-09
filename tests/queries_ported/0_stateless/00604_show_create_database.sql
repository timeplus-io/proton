-- Tags: no-ordinary-database, no-parallel

SET show_uuid=0;

create database if not exists test_00604;
show create database test_00604;
drop database test_00604;
