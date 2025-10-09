/*
Copyright (c) 2013 Monty Program AB. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published
by the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "my_test.h"
#include "mariadb_dyncol.h"

static int create_dyncol_named(MYSQL *mysql)
{
  DYNAMIC_COLUMN dyncol;
  DYNAMIC_COLUMN_VALUE *vals;
  uint i, column_count= 6;
  int rc;
  const char *strval[]= {"Val1", "Val2", "Val3", "Val4", "Val5", "Val6"};
  MYSQL_LEX_STRING keys1[]= {{(char *)"key1", 4}, {(char *)"key2", 4},
                             {(char *)"key3", 4}, {(char *)"key4", 4},
                             {(char *)"key5", 4}, {(char *)"key6", 4}},

                   keys2[]= {{(char *)"key1", 4}, {(char *)"key1", 4},
                             {(char *)"key3", 4}, {(char *)"key4", 4},
                             {(char *)"key5", 4}, {(char *)"key6", 4}},

                   keys3[]= {{(char *)"\x70\x61\x72\x61\x00\x30", 6}, 
                             {(char *)"\x70\x61\x72\x61\x00\x31", 6},
                             {(char *)"\x70\x61\x72\x61\x00\x32", 6},
                             {(char *)"\x70\x61\x72\x61\x00\x33", 6},
                             {(char *)"\x70\x61\x72\x61\x00\x34", 6},
                             {(char *)"\x70\x61\x72\x61\x00\x35", 6}};
  MYSQL_LEX_STRING *my_keys;
  uint my_count;

  vals= (DYNAMIC_COLUMN_VALUE *)malloc(column_count * sizeof(DYNAMIC_COLUMN_VALUE));

  for (i=0; i < column_count; i++)
  {
    vals[i].type= DYN_COL_STRING;
    vals[i].x.string.value.str= (char *)strval[i];
    vals[i].x.string.value.length= strlen(strval[i]);
    vals[i].x.string.charset= (MARIADB_CHARSET_INFO *)mysql->charset;
    diag("%s", keys3[i].str);
  }

  mariadb_dyncol_init(&dyncol);
  rc= mariadb_dyncol_create_many_named(&dyncol, column_count, keys1, vals, 0); 
  mariadb_dyncol_free(&dyncol);
  FAIL_IF(mariadb_dyncol_create_many_named(&dyncol, column_count, keys1, vals, 1) < 0, "Error");
  column_count= 0;
  FAIL_IF(mariadb_dyncol_column_count(&dyncol, &column_count) < 0, "Error");

  FAIL_IF(column_count != 6, "6 columns expected");
  mariadb_dyncol_free(&dyncol);

  rc= mariadb_dyncol_create_many_named(&dyncol, column_count, keys3, vals, 1);
  if (rc < 0) {
    diag("Error!!: %d", rc);
    return FAIL;
  } 
  column_count= 0;
  FAIL_IF(mariadb_dyncol_column_count(&dyncol, &column_count) < 0, "Error");

  FAIL_IF(column_count != 6, "6 columns expected");

  mariadb_dyncol_free(&dyncol);

  /* Now try to add a duplicate key */

  FAIL_IF(mariadb_dyncol_create_many_named(&dyncol, column_count, keys2, vals, 1) >=0, "Error expected");
  mariadb_dyncol_free(&dyncol);

  /* binary keys */
  rc= mariadb_dyncol_create_many_named(&dyncol, column_count, keys3, vals, 1);
  FAIL_IF(rc < 0, "binary keys failed");

  /* get keys*/
  rc= mariadb_dyncol_list_named(&dyncol, &my_count, &my_keys);
  FAIL_IF(rc < 0, "list named failed");

  for (i=0; i < my_count; i++)
  {
    if (memcmp(my_keys[i].str, keys3[i].str, keys3[i].length) != 0)
      diag("error key %d", i);
    vals[i].type=DYN_COL_NULL;
  }
  rc= mariadb_dyncol_update_many_named(&dyncol, column_count, keys3, vals);
  FAIL_IF(rc < 0, "update failed");
  mariadb_dyncol_free(&dyncol);

  keys3[0].str= (char *)"test";
  for (i=0; i < column_count; i++)
    diag("%s", my_keys[i].str);

  free(vals);
  free(my_keys);
  return OK; 
}

static int mdev_4994(MYSQL *unused __attribute__((unused)))
{
  DYNAMIC_COLUMN dyncol;
  uint key= 1;
  DYNAMIC_COLUMN_VALUE val;
  int rc;
  

  val.type= DYN_COL_NULL;

  mariadb_dyncol_init(&dyncol);
  rc= mariadb_dyncol_create_many_num(&dyncol, 1, &key, &val, 0); 
  FAIL_IF(rc < 0, "Unexpected error");
  mariadb_dyncol_free(&dyncol);
  return OK;
}

static int create_dyncol_num(MYSQL *mysql)
{
  DYNAMIC_COLUMN dyncol;
  DYNAMIC_COLUMN_VALUE vals[5];
  uint i, column_count= 5;
  uint my_count;
  MYSQL_LEX_STRING *my_keys;
  DYNAMIC_COLUMN_VALUE *my_vals;
  int rc;
  const char *strval[]= {"Val1", "Val2", "Val3", "Val4", "Val5"};

  uint keys1[5]= {1,2,3,4,5},
       keys2[5]= {1,2,2,4,5};
  MYSQL_LEX_STRING key1= {(char *)"1",1};

  for (i=0; i < column_count; i++)
  {
    vals[i].type= DYN_COL_STRING;
    vals[i].x.string.value.str= (char *)strval[i];
    vals[i].x.string.value.length= strlen(strval[i]);
    vals[i].x.string.charset= (MARIADB_CHARSET_INFO *)mysql->charset;
  }
  FAIL_IF(mariadb_dyncol_create_many_num(&dyncol, column_count, keys1, vals, 1) <0, "Error (keys1)");

  vals[0].x.string.value.str= (char *)strval[1];
  rc= mariadb_dyncol_update_many_named(&dyncol,1, &key1, vals);
  diag("update: %d", rc);

  rc= mariadb_dyncol_unpack(&dyncol, &my_count, &my_keys, &my_vals);
  diag("unpack: %d %d", rc, my_count);

  free(my_keys);
  free(my_vals);

  FAIL_IF(mariadb_dyncol_column_count(&dyncol, &column_count) < 0, "Error");
  FAIL_IF(column_count != 5, "5 columns expected");
  mariadb_dyncol_free(&dyncol);
  FAIL_IF(mariadb_dyncol_create_many_num(&dyncol, column_count, keys2, vals, 1) >=0, "Error expected (keys2)");
  mariadb_dyncol_free(&dyncol);
  return OK;
}

static int mdev_x1(MYSQL *mysql)
{
  int rc;
  uint i;
  uint num_keys[5]= {1,2,3,4,5};
  const char *strval[]= {"Val1", "Val2", "Val3", "Val4", "Val5"};
  DYNAMIC_COLUMN_VALUE vals[5];
  DYNAMIC_COLUMN dynstr;
  MYSQL_LEX_STRING my_key= {(char *)"1", 2};
  uint unpack_columns;
  MYSQL_LEX_STRING *unpack_keys;
  DYNAMIC_COLUMN_VALUE *unpack_vals;

  for (i=0; i < 5; i++)
  {
    vals[i].type= DYN_COL_STRING;
    vals[i].x.string.value.str= (char *)strval[i];
    vals[i].x.string.value.length= strlen(strval[i]);
    vals[i].x.string.charset= (MARIADB_CHARSET_INFO *)mysql->charset;
  }

  mariadb_dyncol_init(&dynstr);

  /* create numeric */
  rc= mariadb_dyncol_create_many_num(&dynstr, 5, num_keys, vals, 1);
  if (rc < 0)
  {
    diag("Error: %d", rc);
    return FAIL;
  }

  /* unpack and print values */
  rc= mariadb_dyncol_unpack(&dynstr, &unpack_columns, &unpack_keys, &unpack_vals);
  if (rc < 0)
  {
    diag("Error: %d", rc);
    return FAIL;
  }

  for (i=0; i < unpack_columns; i++)
    if (memcmp(unpack_vals[i].x.string.value.str, vals[i].x.string.value.str, vals[i].x.string.value.length))
      diag("Error1: key: %1s val: %s %s", unpack_keys[i].str, unpack_vals[i].x.string.value.str, vals[i].x.string.value.str);

  free(unpack_keys);
  free(unpack_vals);

  /* change one value and update with named key */
/*  vals[0].x.string.value.str= strval[1]; */
  rc= mariadb_dyncol_update_many_named(&dynstr, 1, &my_key, vals);
  if (rc < 0)
  {
    diag("Error: %d", rc);
    return FAIL;
  }

  /* unpack and print values */
  rc= mariadb_dyncol_unpack(&dynstr, &unpack_columns, &unpack_keys, &unpack_vals);
  if (rc < 0)
  {
    diag("Error: %d", rc);
    return FAIL;
  }
  diag("Columns: %d", unpack_columns);

  for (i=0; i < unpack_columns; i++)
    diag("Key: %s Len: %lu", unpack_keys[i].str, (unsigned long)unpack_keys[i].length);


  free(unpack_keys);
  free(unpack_vals);

  mariadb_dyncol_free(&dynstr);
  return OK;
}

static int dyncol_column_count(MYSQL *unused __attribute__((unused)))
{
  DYNAMIC_COLUMN dyncol;
  uint column_count= 5;
  int rc;

  mariadb_dyncol_init(&dyncol); /* memset(&dyncol, 0, sizeof(DYNAMIC_COLUMN)) */
  rc= mariadb_dyncol_column_count(&dyncol, &column_count);
  diag("rc=%d", rc);
  FAIL_IF(rc < 0, "unexpected error");
  FAIL_IF(column_count > 0, "Expected column_count=0");  
  return OK;
}

static int dyncol_nested(MYSQL *mysql __attribute__((unused)))
{
  DYNAMIC_COLUMN col1, col2;
  DYNAMIC_COLUMN_VALUE value[2];
  MYSQL_LEX_STRING cols[2]= {{(char *)"0",1},{(char *)"1",1}};
  DYNAMIC_STRING s;

  mariadb_dyncol_init(&col1);
  mariadb_dyncol_init(&col2);

  memset(&value, 0, sizeof(DYNAMIC_COLUMN_VALUE));

  value[0].type= DYN_COL_UINT;
  value[0].x.ulong_value = 17;

  mariadb_dyncol_create_many_named(&col1, 1, cols, value, 0);
  if (mariadb_dyncol_check(&col1) != ER_DYNCOL_OK)
  {
    diag("Error while creating col1");
    return FAIL;
  }

  value[1].type= DYN_COL_DYNCOL;
  value[1].x.string.value.str= col1.str;
  value[1].x.string.value.length= col1.length;
 
  mariadb_dyncol_create_many_named(&col2, 2, cols, value, 0);
  if (mariadb_dyncol_check(&col2) != ER_DYNCOL_OK)
  {
    diag("Error while creating col1");
    return FAIL;
  }
  mariadb_dyncol_json(&col2, &s);
  if (strcmp(s.str, "{\"0\":17,\"1\":{\"0\":17}}") != 0)
  {
    diag("%s != %s", s.str, "{\"0\":17,\"1\":{\"0\":17}}");
    return FAIL;
  }
  ma_dynstr_free(&s);
  mariadb_dyncol_free(&col1);
  return OK;
}

struct my_tests_st my_tests[] = {
  {"mdev_x1", mdev_x1, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"mdev_4994", mdev_4994, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"create_dyncol_named", create_dyncol_named, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"create_dyncol_num", create_dyncol_num, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"dyncol_column_count", dyncol_column_count, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"dyncol_nested", dyncol_nested, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {NULL, NULL, 0, 0, NULL, 0}
};


int main(int argc, char **argv)
{
  if (argc > 1)
   get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  return(exit_status());
}
