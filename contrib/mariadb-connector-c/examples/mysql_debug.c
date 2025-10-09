#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void show_error(MYSQL *mysql)
{
  printf("Error(%d) [%s] \"%s\"", mysql_errno(mysql),
                                  mysql_sqlstate(mysql),
                                  mysql_error(mysql));
  mysql_close(mysql);
  exit(-1);
}

int main(int argc, char *argv[])
{
  MYSQL *mysql;
  const char *query;

  mysql_debug("d:t:O");

  mysql= mysql_init(NULL);

  if (!mysql_real_connect(mysql, "localhost", "example", "example_pw", 
                          "example_db", 0, "/tmp/mysql.sock", 0))
    show_error(mysql);

  query= "DROP TABLE IF EXISTS debug_example";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
 
  query= "CREATE TABLE debug_example (id int not null, my_name varchar(50),"
         "PRIMARY KEY(id))";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);

  mysql_close(mysql);

  return 0;
}
