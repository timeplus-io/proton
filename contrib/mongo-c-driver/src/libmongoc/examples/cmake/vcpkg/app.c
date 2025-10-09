#include <mongoc/mongoc.h>

#include <stdio.h>

int
main (void)
{
   mongoc_init ();
   fprintf (stdout, "Linked with libmongoc %s\n", mongoc_get_version ());
   mongoc_cleanup ();
   return 0;
}
