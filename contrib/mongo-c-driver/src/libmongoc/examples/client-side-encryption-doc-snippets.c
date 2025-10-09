#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

/* Includes code snippets for RST documentation. */
int
main (void)
{
   mongoc_init ();

   {
      /* BEGIN:mongoc_auto_encryption_opts_set_tls_opts. */
      mongoc_auto_encryption_opts_t *ae_opts = mongoc_auto_encryption_opts_new ();
      bson_t *tls_opts = bson_new ();

      BCON_APPEND (tls_opts, "kmip", "{", MONGOC_URI_TLSCAFILE, "ca1.pem", "}");
      BCON_APPEND (tls_opts, "aws", "{", MONGOC_URI_TLSCAFILE, "ca2.pem", "}");
      mongoc_auto_encryption_opts_set_tls_opts (ae_opts, tls_opts);
      /* END:mongoc_auto_encryption_opts_set_tls_opts. */

      bson_destroy (tls_opts);
      mongoc_auto_encryption_opts_destroy (ae_opts);
   }

   {
      /* BEGIN:mongoc_client_encryption_opts_set_tls_opts. */
      mongoc_client_encryption_opts_t *ce_opts = mongoc_client_encryption_opts_new ();
      bson_t *tls_opts = bson_new ();

      BCON_APPEND (tls_opts, "kmip", "{", MONGOC_URI_TLSCAFILE, "ca1.pem", "}");
      BCON_APPEND (tls_opts, "aws", "{", MONGOC_URI_TLSCAFILE, "ca2.pem", "}");
      mongoc_client_encryption_opts_set_tls_opts (ce_opts, tls_opts);
      /* END:mongoc_client_encryption_opts_set_tls_opts. */

      bson_destroy (tls_opts);
      mongoc_client_encryption_opts_destroy (ce_opts);
   }

   mongoc_cleanup ();
   return EXIT_SUCCESS;
}
