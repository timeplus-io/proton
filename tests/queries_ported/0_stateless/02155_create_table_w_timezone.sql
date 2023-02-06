create stream t02155_t64_tz ( a DateTime64(9, America/Chicago)) Engine = Memory; -- { clientError 62 }
create stream t02155_t_tz ( a DateTime(America/Chicago)) Engine = Memory; -- { clientError 62 }

create stream t02155_t64_tz ( a DateTime64(9, 'America/Chicago')) Engine = Memory; 
create stream t02155_t_tz ( a DateTime('America/Chicago')) Engine = Memory; 

drop stream t02155_t64_tz;
drop stream t02155_t_tz;
