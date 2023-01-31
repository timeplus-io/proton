-- since actual user name is unknown, have to perform just smoke tests
select current_user() IS NOT NULL;
select length(current_user()) > 0;
select current_user() = user(), current_user() = USER();
select current_user() = initial_user from system.processes where query like '%$!@#%';
