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
  MYSQL_RES *result;

  mysql= mysql_init(NULL);
  if (!mysql_real_connect(mysql, "localhost", "example", "example_pw", 
                          "example_db", 0, "/tmp/mysql.sock", 0))
    show_error(mysql);

  query= "DROP TABLE IF EXISTS affected_rows";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
 
  query= "CREATE TABLE affected_rows (id int not null, my_name varchar(50),"
         "PRIMARY KEY(id))";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);

  /* Affected rows with INSERT statement */
  query= "INSERT INTO affected_rows VALUES (1, \"First value\"),"
         "(2, \"Second value\")";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  printf("Affected_rows after INSERT: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));

  /* Affected rows with REPLACE statement */
  query= "REPLACE INTO affected_rows VALUES (1, \"First value\"),"
         "(2, \"Second value\")";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  printf("Affected_rows after REPLACE: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));

  /* Affected rows with UPDATE statement */
  query= "UPDATE affected_rows SET id=1 WHERE id=1";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  printf("Affected_rows after UPDATE: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));

  query= "UPDATE affected_rows SET my_name=\"Monty\" WHERE id=1";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  printf("Affected_rows after UPDATE: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));

  /* Affected rows after select */
  query= "SELECT id, my_name FROM affected_rows";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  result= mysql_store_result(mysql);
  printf("Affected_rows after SELECT and storing result set: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));
  mysql_free_result(result);

  /* Affected rows with DELETE statement */
  query= "DELETE FROM affected_rows";
  if (mysql_real_query(mysql, query, strlen(query)))
    show_error(mysql);
  printf("Affected_rows after DELETE: %lu\n",
         (unsigned long) mysql_affected_rows(mysql));

  mysql_close(mysql);
  return 0;
}
