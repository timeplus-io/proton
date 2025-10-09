/************************************************************************************
  Copyright (C) 2017-2019 MariaDB Corporation AB

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/
#ifndef _WIN32
#define _GNU_SOURCE 1
#endif

#ifdef _WIN32
#define HAVE_WINCRYPT
#undef HAVE_OPENSSL
#undef HAVE_GNUTLS
#endif

#if defined(HAVE_OPENSSL) || defined(HAVE_WINCRYPT) || defined(HAVE_GNUTLS)

#include <ma_global.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <memory.h>
#include <errmsg.h>
#include <ma_global.h>
#include <ma_sys.h>
#include <ma_common.h>

#ifndef WIN32
#include <dlfcn.h>
#endif

#if defined(HAVE_WINCRYPT)
#include <windows.h>
#include <wincrypt.h>
#include <bcrypt.h>
#pragma comment(lib, "bcrypt.lib")
#pragma comment(lib, "crypt32.lib")
extern BCRYPT_ALG_HANDLE Sha512Prov;
#elif defined(HAVE_OPENSSL)
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#elif defined(HAVE_GNUTLS)
#include <gnutls/gnutls.h>
#endif

#include <ref10/api.h>
#include <ref10/common.h>
#include <ma_crypt.h>

/* function prototypes */
static int auth_ed25519_client(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql);
static int auth_ed25519_deinit();
static int auth_ed25519_init(char *unused1,
    size_t unused2,
    int unused3,
    va_list);


#ifndef PLUGIN_DYNAMIC
struct st_mysql_client_plugin_AUTHENTICATION client_ed25519_client_plugin=
#else
struct st_mysql_client_plugin_AUTHENTICATION _mysql_client_plugin_declaration_ =
#endif
{
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN,
  MYSQL_CLIENT_AUTHENTICATION_PLUGIN_INTERFACE_VERSION,
  "client_ed25519",
  "Sergei Golubchik, Georg Richter",
  "Ed25519 Authentication Plugin",
  {0,1,0},
  "LGPL",
  NULL,
  auth_ed25519_init,
  auth_ed25519_deinit,
  NULL,
  auth_ed25519_client
};


static int auth_ed25519_client(MYSQL_PLUGIN_VIO *vio, MYSQL *mysql)
{
  unsigned char *packet,
                signature[CRYPTO_BYTES + NONCE_BYTES];
  int pkt_len;

  /*
     Step 1: Server sends nonce
     Step 2: check that packet length is equal to NONCE_BYTES (=32)
     Step 3: Sign the nonce with password
     Steo 4: Send the signature back to server
  */

  /* read and check nonce */
  pkt_len= vio->read_packet(vio, &packet);
  if (pkt_len != NONCE_BYTES)
    return CR_SERVER_HANDSHAKE_ERR;

  /* Sign nonce: the crypto_sign function is part of ref10 */
  crypto_sign(signature, packet, NONCE_BYTES, (unsigned char*)mysql->passwd, strlen(mysql->passwd));

  /* send signature to server */
  if (vio->write_packet(vio, signature, CRYPTO_BYTES))
    return CR_ERROR;

  return CR_OK;
}
/* }}} */

/* {{{ static int auth_ed25519_init */
static int auth_ed25519_init(char *unused1 __attribute__((unused)),
    size_t unused2  __attribute__((unused)),
    int unused3     __attribute__((unused)),
    va_list unused4 __attribute__((unused)))
{
#if defined(HAVE_WINCRYPT)
  BCryptOpenAlgorithmProvider(&Sha512Prov, BCRYPT_SHA512_ALGORITHM, NULL, 0);
#endif
  return 0;
}
/* }}} */

/* {{{ auth_ed25519_deinit */
static int auth_ed25519_deinit()
{
#if defined(HAVE_WINCRYPT)
  BCryptCloseAlgorithmProvider(Sha512Prov, 0);
#endif
  return 0;
}
/* }}} */

#endif  /* defined(HAVE_OPENSSL) || defined(HAVE_WINCRYPT) || defined(HAVE_GNUTLS)*/

