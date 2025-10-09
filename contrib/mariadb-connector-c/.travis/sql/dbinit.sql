CREATE USER 'bob'@'%';
GRANT ALL ON *.* TO 'bob'@'%' with grant option;

FLUSH PRIVILEGES;

CREATE DATABASE ctest;
