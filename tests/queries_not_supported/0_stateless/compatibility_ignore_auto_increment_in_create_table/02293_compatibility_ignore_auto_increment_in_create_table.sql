select 'disable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=false;

select 'create stream failed, column +type +AUTO_INCREMENT, compatibility disabled';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple(); -- {serverError SYNTAX_ERROR}

select 'enable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=true;

select 'create stream, +type +AUTO_INCREMENT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();

select 'create stream, column +AUTO_INCREMENT -type';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (
    id AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM ignore_auto_increment;

select 'create stream, several columns +/-type +AUTO_INCREMENT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (
    id int AUTO_INCREMENT, di AUTO_INCREMENT, s string AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM ignore_auto_increment;

select 'create stream, several columns with different default specifiers';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (
    di DEFAULT 1, id int AUTO_INCREMENT, s string EPHEMERAL
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM ignore_auto_increment;

select 'create stream failed, column +type +DEFAULT +AUTO_INCREMENT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (id int DEFAULT 1 AUTO_INCREMENT) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create stream failed, column -type +DEFAULT +AUTO_INCREMENT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (id int DEFAULT 1 AUTO_INCREMENT) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create stream failed, column +type +AUTO_INCREMENT +DEFAULT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (id int AUTO_INCREMENT DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create stream failed, column -type +AUTO_INCREMENT +DEFAULT';
DROP STREAM IF EXISTS ignore_auto_increment SYNC;
CREATE STREAM ignore_auto_increment (id int AUTO_INCREMENT DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

DROP STREAM IF EXISTS ignore_auto_increment SYNC;
