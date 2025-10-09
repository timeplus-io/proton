drop stream if exists 99094_left;
drop stream if exists 99094_right;

create stream 99094_left(k int, v string);
create stream 99094_right(k int, v string);

--- table latest join table
select * from 99094_left l latest join 99094_right r on l.k = r.k settings query_mode='table'; -- { serverError UNSUPPORTED }
select * from 99094_left l full latest join table(99094_right) r on l.k = r.k settings query_mode='table'; -- { serverError UNSUPPORTED }

--- stream latest join table
select * from 99094_left l latest join table(99094_right) r on l.k = r.k; -- { serverError UNSUPPORTED }
select * from 99094_left l full latest join table(99094_right) r on l.k = r.k; -- { serverError UNSUPPORTED }

drop stream if exists 99094_left;
drop stream if exists 99094_right;
