#include <mongoc/mcd-rpc.h>

#include <mongoc/mongoc-iovec.h>

#include "test-conveniences.h"
#include "TestSuite.h"

#include <inttypes.h>


// clang-format off
#define TEST_DATA_OP_COMPRESSED                                        \
   /*    */ /* header (16 bytes) */                                          \
   /*  0 */ 0x2d, 0x00, 0x00, 0x00, /* messageLength (45)           */       \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)         */       \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)        */       \
   /* 12 */ 0xdc, 0x07, 0x00, 0x00, /* opCode (2012: OP_COMPRESSED) */       \
   /*    */                                                                  \
   /*    */ /* OP_COMPRESSED fields (9 bytes) */                             \
   /* 16 */ 0xdd, 0x07, 0x00, 0x00, /* originalOpcode (2013: OP_MSG) */      \
   /* 20 */ 0x14, 0x00, 0x00, 0x00, /* uncompressedSize (20)         */      \
   /* 24 */ 0x00,                   /* compressorId (0: noop)        */      \
   /*    */                                                                  \
   /*    */ /* compressedMessage (20 bytes) */                               \
   /* 25 */ 0x00, 0x00, 0x00, 0x00, /* flagBits (MONGOC_OP_MSG_FLAG_NONE) */ \
   /* 29 */ 0x00,                   /* Kind 0: Body */                       \
   /* 30 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */             \
   /* 34 */   0x10,                           /*   (int32)    */             \
   /* 35 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*     'kind':  */             \
   /* 40 */     0x00, 0x00, 0x00, 0x00,       /*      0       */             \
   /* 44 */ 0x00                              /* }            */             \
   /* 45 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_MSG_KIND_0                                         \
   /*    */ /* header (16 bytes) */                                           \
   /*  0 */ 0x28, 0x00, 0x00, 0x00, /* messageLength (40)    */               \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)  */               \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096) */               \
   /* 12 */ 0xdd, 0x07, 0x00, 0x00, /* opCode (2013: OP_MSG) */               \
   /*    */                                                                   \
   /*    */ /* flagBits (4 bytes) */                                          \
   /* 16 */ 0x01, 0x00, 0x00, 0x00, /* MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT */ \
   /*    */                                                                   \
   /*    */ /* Section 0 (16 bytes) */                                        \
   /* 20 */ 0x00, /* Kind 0: Body */                                          \
   /* 21 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */              \
   /* 25 */   0x10,                           /*   (int32)    */              \
   /* 26 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*     'kind':  */              \
   /* 31 */     0x00, 0x00, 0x00, 0x00,       /*      0       */              \
   /* 35 */ 0x00,                             /* }            */              \
   /*    */                                                                   \
   /*    */ /* Optional checksum (4 bytes) */                                 \
   /* 36 */ 0x44, 0x33, 0x22, 0x11 /* checksum (287454020) */                 \
   /* 40 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_MSG_KIND_1_SINGLE                                      \
   /*    */ /* header (16 bytes) */                                               \
   /*  0 */ 0x43, 0x00, 0x00, 0x00, /* messageLength (67)    */                   \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)  */                   \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096) */                   \
   /* 12 */ 0xdd, 0x07, 0x00, 0x00, /* opCode (2013: OP_MSG) */                   \
   /*    */                                                                       \
   /*    */ /* flagBits (4 bytes) */                                              \
   /* 16 */ 0x01, 0x00, 0x00, 0x00, /* MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT */     \
   /*    */                                                                       \
   /*    */ /* Section 0 (16 bytes) */                                            \
   /* 20 */ 0x00, /* Kind 0: Body */                                              \
   /* 21 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */                  \
   /* 25 */   0x10,                           /* (int32)      */                  \
   /* 26 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*   'kind':    */                  \
   /* 31 */     0x00, 0x00, 0x00, 0x00,       /*    0         */                  \
   /* 35 */ 0x00,                             /* }            */                  \
   /*    */                                                                       \
   /*    */ /* Section 1 (27 bytes) */                                            \
   /* 36 */ 0x01, /* Kind 1: Document Sequence */                                 \
   /* 37 */ 0x1a, 0x00, 0x00, 0x00,                   /* size (26 bytes)       */ \
   /* 41 */ 0x73, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x00, /* identifier ("single") */ \
   /* 48 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */                  \
   /* 52 */   0x10,                           /*   (int32)    */                  \
   /* 53 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*     'kind':  */                  \
   /* 58 */     0x01, 0x00, 0x00, 0x00,       /*     1        */                  \
   /* 62 */ 0x00,                             /* }            */                  \
   /*    */                                                                       \
   /*    */ /* Optional checksum (4 bytes) */                                     \
   /* 63 */ 0x44, 0x33, 0x22, 0x11 /* checksum (287454020) */                     \
   /* 67 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_MSG_KIND_1_MULTIPLE                                     \
   /*     */ /* header (16 bytes) */                                               \
   /*   0 */ 0x6e, 0x00, 0x00, 0x00, /* messageLength (110)   */                   \
   /*   4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)  */                   \
   /*   8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096) */                   \
   /*  12 */ 0xdd, 0x07, 0x00, 0x00, /* opCode (2013: OP_MSG) */                   \
   /*     */                                                                       \
   /*     */ /* flagBits (4 bytes) */                                              \
   /*  16 */ 0x01, 0x00, 0x00, 0x00, /* MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT */     \
   /*     */                                                                       \
   /*     */ /* Section 0 (16 bytes) */                                            \
   /*  20 */ 0x00, /* Kind 0: Body. */                                             \
   /*  21 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */                  \
   /*  25 */   0x10,                           /*   (int32)    */                  \
   /*  26 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*     'kind':  */                  \
   /*  31 */     0x00, 0x00, 0x00, 0x00,       /*      0       */                  \
   /*  35 */ 0x00,                             /* }            */                  \
   /*     */                                                                       \
   /*     */ /* Section 1 (26 bytes) */                                            \
   /*  36 */ 0x01, /* Kind 1: Document Sequence */                                 \
   /*  37 */ 0x19, 0x00, 0x00, 0x00,             /* size (25 bytes)      */        \
   /*  41 */ 0x66, 0x69, 0x72, 0x73, 0x74, 0x00, /* identifier ("first") */        \
   /*  47 */ 0x0f, 0x00, 0x00, 0x00,           /* (15 bytes) { */                  \
   /*  51 */   0x10,                           /*   (int32)    */                  \
   /*  52 */     0x6b, 0x69, 0x6e, 0x64, 0x00, /*     'kind':  */                  \
   /*  57 */     0x01, 0x00, 0x00, 0x00,       /*      1       */                  \
   /*  61 */ 0x00,                             /* }            */                  \
   /*     */                                                                       \
   /*     */ /* Section 2 (44 bytes) */                                            \
   /*  62 */ 0x01, /* Kind 1: Document Sequence */                                 \
   /*  63 */ 0x2b, 0x00, 0x00, 0x00,                   /* size (43 bytes)       */ \
   /*  67 */ 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x00, /* identifier ("second") */ \
   /*  74 */ 0x10, 0x00, 0x00, 0x00,                 /* (16 bytes) { */            \
   /*  78 */   0x10,                                 /*   (int32)    */            \
   /*  79 */     0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */            \
   /*  85 */     0x00, 0x00, 0x00, 0x00,             /*      0       */            \
   /*  89 */ 0x00,                                   /* }            */            \
   /*  90 */ 0x10, 0x00, 0x00, 0x00,                 /* (16 bytes) { */            \
   /*  94 */   0x10,                                 /*   (int32)    */            \
   /*  95 */     0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */            \
   /* 101 */     0x01, 0x00, 0x00, 0x00,             /*      1       */            \
   /* 105 */ 0x00,                                   /* }            */            \
   /*     */                                                                       \
   /*     */ /* Optional checksum (4 bytes) */                                     \
   /* 106 */ 0x44, 0x33, 0x22, 0x11 /* checksum (287454020) */                     \
   /* 110 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_REPLY                                                                 \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x44, 0x00, 0x00, 0x00, /* messageLength (68)    */                                  \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)  */                                  \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096) */                                  \
   /* 12 */ 0x01, 0x00, 0x00, 0x00, /* opCode (1: OP_REPLY)  */                                  \
   /*    */                                                                                      \
   /*    */ /* OP_REPLY fields (52 bytes) */                                                     \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* responseFlags (0: none)        */ \
   /* 20 */ 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, /* cursorID (1234605616436508552) */ \
   /* 28 */ 0x00, 0x00, 0x00, 0x00,                         /* startingFrom (0)               */ \
   /* 32 */ 0x02, 0x00, 0x00, 0x00,                         /* numberReturned (2)             */ \
   /*    */                                                                                      \
   /*    */ /* documents (32 bytes) */                                                           \
   /* 36 */ 0x10, 0x00, 0x00, 0x00,                   /* (16 bytes) { */                         \
   /* 40 */    0x10,                                  /*   (int32)    */                         \
   /* 41 */       0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */                         \
   /* 47 */       0x00, 0x00, 0x00, 0x00,             /*      0       */                         \
   /* 51 */ 0x00,                                     /* }            */                         \
   /* 52 */ 0x10, 0x00, 0x00, 0x00,                   /* (16 bytes) { */                         \
   /* 56 */    0x10,                                  /*   (int32)    */                         \
   /* 57 */       0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */                         \
   /* 63 */       0x01, 0x00, 0x00, 0x00,             /*      1       */                         \
   /* 67 */ 0x00                                      /* }            */                         \
   /* 68 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_UPDATE                                                                \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x3e, 0x00, 0x00, 0x00, /* messageLength (62)       */                               \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)     */                               \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)    */                               \
   /* 12 */ 0xd1, 0x07, 0x00, 0x00, /* opCode (2001: OP_UPDATE) */                               \
   /*    */                                                                                      \
   /*    */ /* OP_UPDATE fields (46 bytes) */                                                    \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* ZERO                           */ \
   /* 20 */ 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x00, /* fullCollectionName ("db.coll") */ \
   /* 28 */ 0x00, 0x00, 0x00, 0x00,                         /* flags (0: none)                */ \
   /*    */                                                                                      \
   /*    */ /* selector (16 bytes) */                                                            \
   /* 32 */ 0x10, 0x00, 0x00, 0x00,                                     /* (16 bytes) {    */    \
   /* 36 */    0x08,                                                    /*   (boolean)     */    \
   /* 37 */       0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x00, /*     'selector': */    \
   /* 46 */       0x00,                                                 /*     false       */    \
   /* 47 */ 0x00,                                                       /* }               */    \
   /*    */                                                                                      \
   /*    */ /* update (14 bytes) */                                                              \
   /* 48 */ 0x0e, 0x00, 0x00, 0x00,                                     /* (14 bytes)      */    \
   /* 52 */    0x08,                                                    /*   (boolean)     */    \
   /* 53 */       0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x00,             /*     'update':   */    \
   /* 54 */       0x01,                                                 /*     true        */    \
   /* 61 */ 0x00                                                        /* }               */    \
   /* 62 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_INSERT                                                                \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x3c, 0x00, 0x00, 0x00, /* messageLength (60)       */                               \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)     */                               \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)    */                               \
   /* 12 */ 0xd2, 0x07, 0x00, 0x00, /* opCode (2002: OP_INSERT) */                               \
   /*    */                                                                                      \
   /*    */ /* OP_INSERT fields (48 bytes ) */                                                   \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* flags (0: none)                */ \
   /* 20 */ 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x00, /* fullCollectionName ("db.coll") */ \
   /*    */                                                                                      \
   /*    */ /* documents (32 bytes) */                                                           \
   /* 28 */ 0x10, 0x00, 0x00, 0x00,                   /* (16 bytes) { */                         \
   /* 32 */    0x10,                                  /*   (int32)    */                         \
   /* 33 */       0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */                         \
   /* 39 */       0x00, 0x00, 0x00, 0x00,             /*      0       */                         \
   /* 43 */ 0x00,                                     /* }            */                         \
   /* 44 */ 0x10, 0x00, 0x00, 0x00,                   /* (16 bytes) { */                         \
   /* 48 */    0x10,                                  /*   (int32)    */                         \
   /* 49 */       0x69, 0x6e, 0x64, 0x65, 0x78, 0x00, /*     'index': */                         \
   /* 55 */       0x01, 0x00, 0x00, 0x00,             /*      1       */                         \
   /* 59 */ 0x00                                      /* }            */                         \
   /* 60 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_QUERY                                                                 \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x41, 0x00, 0x00, 0x00, /* messageLength (65)      */                                \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)    */                                \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)   */                                \
   /* 12 */ 0xd4, 0x07, 0x00, 0x00, /* opCode (2004: OP_QUERY) */                                \
   /*    */                                                                                      \
   /*    */ /* OP_QUERY fields (49 bytes) */                                                     \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* flags (0: none)                */ \
   /* 20 */ 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x00, /* fullCollectionName ("db.coll") */ \
   /* 28 */ 0x00, 0x00, 0x00, 0x00,                         /* numberToSkip (0)               */ \
   /* 32 */ 0x00, 0x00, 0x00, 0x00,                         /* numberToReturn (0)             */ \
   /*    */                                                                                      \
   /*    */ /* query (13 bytes) */                                                               \
   /* 36 */ 0x0d, 0x00, 0x00, 0x00,                   /* (13 bytes) {    */                      \
   /* 40 */    0x08,                                  /*   (boolean)     */                      \
   /* 41 */       0x71, 0x75, 0x65, 0x72, 0x79, 0x00, /*     'query':    */                      \
   /* 47 */       0x00,                               /*      false      */                      \
   /* 48 */ 0x00,                                     /* }               */                      \
   /*    */                                                                                      \
   /*    */ /* Optional returnFieldsSelector (16 bytes) */                                       \
   /* 49 */ 0x10, 0x00, 0x00, 0x00,                                     /* (16 bytes) {    */    \
   /* 53 */    0x08,                                                    /*   (boolean)     */    \
   /* 54 */       0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x00, /*     'selector': */    \
   /* 63 */       0x01,                                                 /*      true       */    \
   /* 64 */ 0x00                                                        /* }               */    \
   /* 65 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_GET_MORE                                                              \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x28, 0x00, 0x00, 0x00, /* messageLength (40)         */                             \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)       */                             \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)      */                             \
   /* 12 */ 0xd5, 0x07, 0x00, 0x00, /* opCode (2005: OP_GET_MORE) */                             \
   /*    */                                                                                      \
   /*    */ /* OP_GET_MORE fields (24 bytes) */                                                  \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* ZERO                           */ \
   /* 20 */ 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x00, /* fullCollectionName ("db.coll") */ \
   /* 28 */ 0x00, 0x00, 0x00, 0x00,                         /* numberToReturn (0)             */ \
   /* 32 */ 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11  /* cursorID (1234605616436508552) */ \
   /* 40 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_DELETE                                                                \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x30, 0x00, 0x00, 0x00, /* messageLength (48)       */                               \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)     */                               \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)    */                               \
   /* 12 */ 0xd6, 0x07, 0x00, 0x00, /* opCode (2006: OP_DELETE) */                               \
   /*    */                                                                                      \
   /*    */ /* OP_DELETE fields (16 bytes) */                                                    \
   /* 16 */ 0x00, 0x00, 0x00, 0x00,                         /* ZERO                           */ \
   /* 20 */ 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x00, /* fullCollectionName ("db.coll") */ \
   /* 28 */ 0x00, 0x00, 0x00, 0x00,                         /* flags (0: none)                */ \
   /*    */                                                                                      \
   /*    */ /* selector (16 bytes) */                                                            \
   /* 32 */ 0x10, 0x00, 0x00, 0x00,                                     /* (16 bytes) {    */    \
   /* 36 */    0x08,                                                    /*   (boolean)     */    \
   /* 37 */       0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x00, /*     'selector': */    \
   /* 46 */       0x00,                                                 /*      false      */    \
   /* 47 */ 0x00                                                        /* }               */    \
   /* 48 */
// clang-format on

// clang-format off
#define TEST_DATA_OP_KILL_CURSORS                                                          \
   /*    */ /* header (16 bytes) */                                                              \
   /*  0 */ 0x28, 0x00, 0x00, 0x00, /* messageLength (40)             */                         \
   /*  4 */ 0x04, 0x03, 0x02, 0x01, /* requestID (16909060)           */                         \
   /*  8 */ 0x08, 0x07, 0x06, 0x05, /* responseTo (84281096)          */                         \
   /* 12 */ 0xd7, 0x07, 0x00, 0x00, /* opCode (2007: OP_KILL_CURSORS) */                         \
   /*    */                                                                                      \
   /*    */ /* OP_KILL_CURSORS fields (8 bytes) */                                               \
   /* 16 */ 0x00, 0x00, 0x00, 0x00, /* ZERO                 */                                   \
   /* 20 */ 0x02, 0x00, 0x00, 0x00, /* numberOfCursorIds (2)*/                                   \
   /*    */                                                                                      \
   /*    */ /* cursorIDs (16 bytes) */                                                           \
   /* 24 */ 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, /* cursorID (1230066625199609624) */ \
   /* 32 */ 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21  /* cursorID (2387509390608836392) */ \
   /* 40 */
// clang-format on


#define ASSERT_RPC_MESSAGE_RESULT(rpc, data_begin, data_end, data_len)                      \
   if (1) {                                                                                 \
      const size_t parsed_len = (size_t) ((const uint8_t *) data_end - data_begin);         \
      if (rpc) {                                                                            \
         ASSERT_WITH_MSG (parsed_len == data_len,                                           \
                          "converted only %zu bytes despite %zu bytes of valid input data", \
                          parsed_len,                                                       \
                          data_len);                                                        \
      } else {                                                                              \
         ASSERT_WITH_MSG (rpc,                                                              \
                          "failed to convert valid input data into an RPC "                 \
                          "message due to byte %zu of %zu",                                 \
                          parsed_len,                                                       \
                          data_len);                                                        \
      }                                                                                     \
   } else                                                                                   \
      (void) 0


static int32_t
_int32_from_le (const void *data)
{
   BSON_ASSERT_PARAM (data);
   return bson_iter_int32_unsafe (&(bson_iter_t){.raw = data});
}

static int64_t
_int64_from_le (const void *data)
{
   BSON_ASSERT_PARAM (data);
   return bson_iter_int64_unsafe (&(bson_iter_t){.raw = data});
}


static void
test_rpc_message_from_data_op_compressed_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_COMPRESSED};
   const size_t data_len = sizeof (data);

   // Valid test input data.
   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_COMPRESSED);

      ASSERT_CMPINT32 (mcd_rpc_op_compressed_get_original_opcode (rpc), ==, MONGOC_OP_CODE_MSG);

      ASSERT_CMPINT32 (mcd_rpc_op_compressed_get_uncompressed_size (rpc), ==, 20);

      ASSERT_CMPUINT (mcd_rpc_op_compressed_get_compressor_id (rpc), ==, MONGOC_OP_COMPRESSED_COMPRESSOR_ID_NOOP);

      const uint8_t *const compressed_message = mcd_rpc_op_compressed_get_compressed_message (rpc);
      ASSERT_CMPSIZE_T ((size_t) (compressed_message - data), ==, 25u);

      ASSERT_CMPSIZE_T (mcd_rpc_op_compressed_get_compressed_message_length (rpc), ==, 20u);

      mcd_rpc_message_destroy (rpc);
   }

   // Test that compressorId is being parsed correctly.
   {
      data[24] = MONGOC_OP_COMPRESSED_COMPRESSOR_ID_SNAPPY;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPUINT (mcd_rpc_op_compressed_get_compressor_id (rpc), ==, MONGOC_OP_COMPRESSED_COMPRESSOR_ID_SNAPPY);
      mcd_rpc_message_destroy (rpc);

      data[24] = MONGOC_OP_COMPRESSED_COMPRESSOR_ID_NOOP;
   }
}


static void
_test_rpc_message_from_data_op_msg_valid (uint8_t *data,
                                          size_t data_len,
                                          void (*test) (const uint8_t *data, size_t data_len, bool with_checksum))
{
   ASSERT_WITH_MSG ((data[16] & MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT) != 0,
                    "test input data did not set MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT");

   // Test with and without the optional checksum by temporarily modifying the
   // data and data length accordingly.
   {
      data[0] = (uint8_t) (data[0] - 4u); // Reduce messageLength.
      data[16] = (uint8_t) (data[16] & ~MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT);

      test (data, data_len - 4u, false);

      data[0] = (uint8_t) (data[0] + 4u); // Revert messageLength.
      data[16] = (uint8_t) (data[16] | MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT);
   }

   test (data, data_len, true);
}

static void
_test_rpc_message_from_data_op_msg_valid_kind_0 (const uint8_t *data, size_t data_len, bool with_checksum)
{
   const void *data_end = NULL;
   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

   ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
   ASSERT (bson_in_range_unsigned (int32_t, data_len));
   ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
   ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
   ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
   ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_MSG);

   if (with_checksum) {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT);
   } else {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_NONE);
   }

   ASSERT_CMPSIZE_T (mcd_rpc_op_msg_get_sections_count (rpc), ==, 1u);

   ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 0u), ==, 0u);
   const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 0u);
   ASSERT_CMPINT32 (section_len, ==, 15);
   const void *const body = mcd_rpc_op_msg_section_get_body (rpc, 0u);
   ASSERT (body);

   bson_t bson;
   ASSERT (bson_init_static (&bson, body, (size_t) section_len));
   ASSERT_MATCH (&bson, "{'kind': 0}");

   if (with_checksum) {
      const uint32_t *checksum = mcd_rpc_op_msg_get_checksum (rpc);
      ASSERT (checksum);
      ASSERT_CMPUINT32 (*checksum, ==, 287454020u);
   } else {
      ASSERT (!mcd_rpc_op_msg_get_checksum (rpc));
   }

   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_from_data_op_msg_valid_kind_0 (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_0};

   _test_rpc_message_from_data_op_msg_valid (data, sizeof (data), _test_rpc_message_from_data_op_msg_valid_kind_0);
}

static void
_test_rpc_message_from_data_op_msg_valid_kind_1_single (const uint8_t *data, size_t data_len, bool with_checksum)
{
   const void *data_end = NULL;
   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

   ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
   ASSERT (bson_in_range_unsigned (int32_t, data_len));
   ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
   ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
   ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
   ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_MSG);

   if (with_checksum) {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT);
   } else {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_NONE);
   }

   ASSERT_CMPSIZE_T (mcd_rpc_op_msg_get_sections_count (rpc), ==, 2u);

   // Section 0.
   {
      ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 0u), ==, 0u);
      const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 0u);
      ASSERT_CMPINT32 (section_len, ==, 15);
      const void *const body = mcd_rpc_op_msg_section_get_body (rpc, 0u);
      ASSERT (body);

      bson_t bson;
      ASSERT (bson_init_static (&bson, body, (size_t) section_len));
      ASSERT_MATCH (&bson, "{'kind': 0}");
   }

   // Section 1.
   {
      ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 1u), ==, 1u);
      const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 1u);
      ASSERT_CMPINT32 (section_len, ==, 26);
      ASSERT_CMPSTR (mcd_rpc_op_msg_section_get_identifier (rpc, 1u), "single");
      const void *const sequence = mcd_rpc_op_msg_section_get_document_sequence (rpc, 1u);
      ASSERT (sequence);
      ASSERT_CMPSIZE_T (mcd_rpc_op_msg_section_get_document_sequence_length (rpc, 1u), ==, 15u);

      const int32_t bson_len = _int32_from_le (sequence);
      ASSERT_CMPINT32 (bson_len, ==, 15);

      bson_t bson;
      ASSERT (bson_init_static (&bson, sequence, (size_t) bson_len));
      ASSERT_MATCH (&bson, "{'kind': 1}");
   }

   if (with_checksum) {
      const uint32_t *checksum = mcd_rpc_op_msg_get_checksum (rpc);
      ASSERT (checksum);
      ASSERT_CMPUINT32 (*checksum, ==, 287454020u);
   } else {
      ASSERT (!mcd_rpc_op_msg_get_checksum (rpc));
   }

   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_from_data_op_msg_valid_kind_1_single (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_SINGLE};

   _test_rpc_message_from_data_op_msg_valid (
      data, sizeof (data), _test_rpc_message_from_data_op_msg_valid_kind_1_single);
}

static void
_test_rpc_message_from_data_op_msg_valid_kind_1_multiple (const uint8_t *data, size_t data_len, bool with_checksum)
{
   const void *data_end = NULL;
   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

   ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
   ASSERT (bson_in_range_unsigned (int32_t, data_len));
   ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
   ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
   ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
   ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_MSG);

   if (with_checksum) {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT);
   } else {
      ASSERT_CMPUINT32 (mcd_rpc_op_msg_get_flag_bits (rpc), ==, MONGOC_OP_MSG_FLAG_NONE);
   }

   ASSERT_CMPSIZE_T (mcd_rpc_op_msg_get_sections_count (rpc), ==, 3u);

   // Section 0.
   {
      ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 0u), ==, 0u);
      const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 0u);
      ASSERT_CMPINT32 (section_len, ==, 15);
      const void *const body = mcd_rpc_op_msg_section_get_body (rpc, 0u);
      ASSERT (body);

      bson_t bson;
      ASSERT (bson_init_static (&bson, body, (size_t) section_len));
      ASSERT_MATCH (&bson, "{'kind': 0}");
   }

   // Section 1.
   {
      ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 1u), ==, 1u);
      const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 1u);
      ASSERT_CMPINT32 (section_len, ==, 25);
      ASSERT_CMPSTR (mcd_rpc_op_msg_section_get_identifier (rpc, 1u), "first");
      const void *const sequence = mcd_rpc_op_msg_section_get_document_sequence (rpc, 1u);
      ASSERT (sequence);

      const int32_t bson_len = _int32_from_le (sequence);
      ASSERT_CMPINT32 (bson_len, ==, 15);

      bson_t bson;
      ASSERT (bson_init_static (&bson, sequence, (size_t) bson_len));
      ASSERT_MATCH (&bson, "{'kind': 1}");
   }

   // Section 2.
   {
      ASSERT_CMPUINT (mcd_rpc_op_msg_section_get_kind (rpc, 2u), ==, 1u);
      const int32_t section_len = mcd_rpc_op_msg_section_get_length (rpc, 2u);
      ASSERT_CMPINT32 (section_len, ==, 43);
      ASSERT_CMPSTR (mcd_rpc_op_msg_section_get_identifier (rpc, 2u), "second");
      const uint8_t *const sequence = mcd_rpc_op_msg_section_get_document_sequence (rpc, 2u);
      ASSERT (sequence);

      // BSON objects, index 0.
      {
         const uint8_t *const doc_0 = sequence;
         const int32_t bson_len = _int32_from_le (doc_0);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_0, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 0}");
         bson_destroy (&bson);
      }

      // BSON objects, index 1.
      {
         const uint8_t *const doc_1 = sequence + 16;
         const int32_t bson_len = _int32_from_le (doc_1);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_1, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 1}");
         bson_destroy (&bson);
      }
   }

   if (with_checksum) {
      const uint32_t *checksum = mcd_rpc_op_msg_get_checksum (rpc);
      ASSERT (checksum);
      ASSERT_CMPUINT32 (*checksum, ==, 287454020u);
   } else {
      ASSERT (!mcd_rpc_op_msg_get_checksum (rpc));
   }

   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_from_data_op_msg_valid_kind_1_multiple (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_MULTIPLE};

   _test_rpc_message_from_data_op_msg_valid (
      data, sizeof (data), _test_rpc_message_from_data_op_msg_valid_kind_1_multiple);
}

static void
test_rpc_message_from_data_op_msg_valid (void)
{
   test_rpc_message_from_data_op_msg_valid_kind_0 ();
   test_rpc_message_from_data_op_msg_valid_kind_1_single ();
   test_rpc_message_from_data_op_msg_valid_kind_1_multiple ();
}

static void
test_rpc_message_from_data_op_reply_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_REPLY};
   const size_t data_len = sizeof (data);

   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_REPLY);

      ASSERT_CMPINT32 (mcd_rpc_op_reply_get_response_flags (rpc), ==, MONGOC_OP_REPLY_RESPONSE_FLAG_NONE);

      ASSERT_CMPINT64 (mcd_rpc_op_reply_get_cursor_id (rpc), ==, 1234605616436508552);

      ASSERT_CMPINT32 (mcd_rpc_op_reply_get_starting_from (rpc), ==, 0);

      ASSERT_CMPINT32 (mcd_rpc_op_reply_get_number_returned (rpc), ==, 2);

      const uint8_t *const documents = mcd_rpc_op_reply_get_documents (rpc);
      ASSERT_CMPSIZE_T ((size_t) (documents - data), ==, 36u);

      ASSERT_CMPSIZE_T (mcd_rpc_op_reply_get_documents_len (rpc), ==, 32u);

      // Documents, index 0.
      {
         const uint8_t *const doc_0 = documents;
         const int32_t bson_len = _int32_from_le (doc_0);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_0, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 0}");
         bson_destroy (&bson);
      }

      // Documents, index 1.
      {
         const uint8_t *const doc_1 = documents + 16;
         const int32_t bson_len = _int32_from_le (doc_1);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_1, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 1}");
         bson_destroy (&bson);
      }

      mcd_rpc_message_destroy (rpc);
   }

   // Test that responseFlags is being parsed correctly.
   {
      data[16] = MONGOC_OP_REPLY_RESPONSE_FLAG_CURSOR_NOT_FOUND;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_reply_get_response_flags (rpc), ==, MONGOC_OP_REPLY_RESPONSE_FLAG_CURSOR_NOT_FOUND);
      mcd_rpc_message_destroy (rpc);

      data[16] = MONGOC_OP_REPLY_RESPONSE_FLAG_NONE;
   }

   // Test that startingFrom is being parsed correctly.
   {
      data[28] = 1u;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_reply_get_starting_from (rpc), ==, 1);
      mcd_rpc_message_destroy (rpc);

      data[28] = 0u;
   }

   // Test that documents are being parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 32u); // Exclude documents.
      data[32] = 0x00u;                    // Set numberReturned to 0.

      {
         mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
         ASSERT (rpc);
         ASSERT_CMPINT32 (mcd_rpc_op_reply_get_number_returned (rpc), ==, 0);
         ASSERT_CMPSIZE_T (mcd_rpc_op_reply_get_documents_len (rpc), ==, 0u);
         ASSERT (mcd_rpc_op_reply_get_documents (rpc) == NULL);
         mcd_rpc_message_destroy (rpc);
      }

      data[32] = 0x02u;                    // Revert numberReturned to 2.
      data[0] = (uint8_t) (data[0] + 32u); // Restore documents.
   }
}

static void
test_rpc_message_from_data_op_update_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_UPDATE};
   const size_t data_len = sizeof (data);

   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_UPDATE);

      ASSERT_CMPSTR (mcd_rpc_op_update_get_full_collection_name (rpc), "db.coll");

      ASSERT_CMPINT32 (mcd_rpc_op_update_get_flags (rpc), ==, MONGOC_OP_UPDATE_FLAG_NONE);

      {
         const uint8_t *const selector = mcd_rpc_op_update_get_selector (rpc);
         ASSERT_CMPSIZE_T ((size_t) (selector - data), ==, 32u);

         const int32_t selector_len = _int32_from_le (selector);
         ASSERT_CMPINT32 (selector_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, selector, (size_t) selector_len));
         ASSERT_MATCH (&bson, "{'selector': false}");
         bson_destroy (&bson);
      }

      {
         const uint8_t *const update = mcd_rpc_op_update_get_update (rpc);
         ASSERT_CMPSIZE_T ((size_t) (update - data), ==, 48u);

         const int32_t update_len = _int32_from_le (update);
         ASSERT_CMPINT32 (update_len, ==, 14);

         bson_t bson;
         ASSERT (bson_init_static (&bson, update, (size_t) update_len));
         ASSERT_MATCH (&bson, "{'update': true}");
         bson_destroy (&bson);
      }

      mcd_rpc_message_destroy (rpc);
   }

   // Test that flags is being parsed correctly.
   {
      data[28] = MONGOC_OP_UPDATE_FLAG_UPSERT;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_update_get_flags (rpc), ==, MONGOC_OP_UPDATE_FLAG_UPSERT);
      mcd_rpc_message_destroy (rpc);

      data[28] = MONGOC_OP_UPDATE_FLAG_NONE;
   }
}

static void
test_rpc_message_from_data_op_insert_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_INSERT};
   const size_t data_len = sizeof (data);

   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_INSERT);

      ASSERT_CMPINT32 (mcd_rpc_op_insert_get_flags (rpc), ==, MONGOC_OP_INSERT_FLAG_NONE);

      ASSERT_CMPSTR (mcd_rpc_op_insert_get_full_collection_name (rpc), "db.coll");

      const uint8_t *const documents = mcd_rpc_op_insert_get_documents (rpc);
      ASSERT (documents);
      ASSERT_CMPSIZE_T (mcd_rpc_op_insert_get_documents_len (rpc), ==, 32u);

      // Documents, index 0.
      {
         const uint8_t *const doc_0 = documents;
         const int32_t bson_len = _int32_from_le (doc_0);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_0, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 0}");
         bson_destroy (&bson);
      }

      // Documents, index 1.
      {
         const uint8_t *const doc_1 = documents + 16;
         const int32_t bson_len = _int32_from_le (doc_1);
         ASSERT_CMPINT32 (bson_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, doc_1, (size_t) bson_len));
         ASSERT_MATCH (&bson, "{'index': 1}");
         bson_destroy (&bson);
      }

      mcd_rpc_message_destroy (rpc);
   }

   // Test that flags is being parsed correctly.
   {
      data[16] = MONGOC_OP_INSERT_FLAG_CONTINUE_ON_ERROR;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_insert_get_flags (rpc), ==, MONGOC_OP_INSERT_FLAG_CONTINUE_ON_ERROR);
      mcd_rpc_message_destroy (rpc);

      data[16] = MONGOC_OP_INSERT_FLAG_NONE;
   }

   // Test that documents are being parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 16u); // Exclude document 1.

      {
         mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
         ASSERT (rpc);
         ASSERT_CMPSIZE_T (mcd_rpc_op_insert_get_documents_len (rpc), ==, 16u);
         mcd_rpc_message_destroy (rpc);
      }

      data[0] = (uint8_t) (data[0] + 16u); // Restore document 1.
   }
}

static void
test_rpc_message_from_data_op_query_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_QUERY};
   const size_t data_len = sizeof (data);

   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_QUERY);

      ASSERT_CMPINT32 (mcd_rpc_op_query_get_flags (rpc), ==, MONGOC_OP_QUERY_FLAG_NONE);

      ASSERT_CMPSTR (mcd_rpc_op_query_get_full_collection_name (rpc), "db.coll");

      ASSERT_CMPINT32 (mcd_rpc_op_query_get_number_to_skip (rpc), ==, 0);

      ASSERT_CMPINT32 (mcd_rpc_op_query_get_number_to_return (rpc), ==, 0);

      {
         const uint8_t *const query = mcd_rpc_op_query_get_query (rpc);
         ASSERT_CMPSIZE_T ((size_t) (query - data), ==, 36u);

         const int32_t query_len = _int32_from_le (query);
         ASSERT_CMPINT32 (query_len, ==, 13);

         bson_t bson;
         ASSERT (bson_init_static (&bson, query, (size_t) query_len));
         ASSERT_MATCH (&bson, "{'query': false}");
         bson_destroy (&bson);
      }

      {
         const uint8_t *const selector = mcd_rpc_op_query_get_return_fields_selector (rpc);
         ASSERT_CMPSIZE_T ((size_t) (selector - data), ==, 49u);

         const int32_t selector_len = _int32_from_le (selector);
         ASSERT_CMPINT32 (selector_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, selector, (size_t) selector_len));
         ASSERT_MATCH (&bson, "{'selector': true}");
         bson_destroy (&bson);
      }

      mcd_rpc_message_destroy (rpc);
   }

   // Test that flags is being parsed correctly.
   {
      data[16] = MONGOC_OP_QUERY_FLAG_TAILABLE_CURSOR;

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_query_get_flags (rpc), ==, MONGOC_OP_QUERY_FLAG_TAILABLE_CURSOR);
      mcd_rpc_message_destroy (rpc);

      data[16] = MONGOC_OP_QUERY_FLAG_NONE;
   }

   // Test that numberToSkip is being parsed correctly.
   {
      data[28] = 1; // Set numberToSkip to 1.

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_query_get_number_to_skip (rpc), ==, 1);
      mcd_rpc_message_destroy (rpc);

      data[28] = 0; // Restore numberToSkip.
   }

   // Test that numberToReturn is being parsed correctly.
   {
      data[32] = 1; // Set numberToReturn to 1.

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_query_get_number_to_return (rpc), ==, 1);
      mcd_rpc_message_destroy (rpc);

      data[32] = 0; // Restore numberToReturn.
   }

   // Test that returnFieldSelector is optional.
   {
      data[0] = (uint8_t) (data[0] - 16u); // Omit returnFieldSelector.

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT (!mcd_rpc_op_query_get_return_fields_selector (rpc));
      mcd_rpc_message_destroy (rpc);

      data[0] = (uint8_t) (data[0] + 16u); // Restore returnFieldSelector.
   }
}

static void
test_rpc_message_from_data_op_get_more_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_GET_MORE};
   const size_t data_len = sizeof (data);

   // Valid test input data.
   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_GET_MORE);

      ASSERT_CMPSTR (mcd_rpc_op_get_more_get_full_collection_name (rpc), "db.coll");

      ASSERT_CMPINT32 (mcd_rpc_op_get_more_get_number_to_return (rpc), ==, 0);

      ASSERT_CMPINT64 (mcd_rpc_op_get_more_get_cursor_id (rpc), ==, 1234605616436508552);

      mcd_rpc_message_destroy (rpc);
   }
}

static void
test_rpc_message_from_data_op_delete_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_DELETE};
   const size_t data_len = sizeof (data);

   // Valid test input data.
   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_DELETE);

      ASSERT_CMPSTR (mcd_rpc_op_delete_get_full_collection_name (rpc), "db.coll");

      ASSERT_CMPINT32 (mcd_rpc_op_delete_get_flags (rpc), ==, MONGOC_OP_DELETE_FLAG_NONE);

      {
         const uint8_t *const selector = mcd_rpc_op_delete_get_selector (rpc);
         ASSERT_CMPSIZE_T ((size_t) (selector - data), ==, 32u);

         const int32_t selector_len = _int32_from_le (selector);
         ASSERT_CMPINT32 (selector_len, ==, 16);

         bson_t bson;
         ASSERT (bson_init_static (&bson, selector, (size_t) selector_len));
         ASSERT_MATCH (&bson, "{'selector': false}");
         bson_destroy (&bson);
      }

      mcd_rpc_message_destroy (rpc);
   }
}

static void
test_rpc_message_from_data_op_kill_cursors_valid (void)
{
   uint8_t data[] = {TEST_DATA_OP_KILL_CURSORS};
   const size_t data_len = sizeof (data);

   // Valid test input data.
   {
      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

      ASSERT_RPC_MESSAGE_RESULT (rpc, data, data_end, data_len);
      ASSERT (bson_in_range_unsigned (int32_t, data_len));
      ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, (int32_t) data_len);
      ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 16909060);
      ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 84281096);
      ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_KILL_CURSORS);

      ASSERT_CMPINT32 (mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (rpc), ==, 2);

      const int64_t *const cursor_ids = mcd_rpc_op_kill_cursors_get_cursor_ids (rpc);

      ASSERT_CMPINT64 (cursor_ids[0], ==, 1230066625199609624);
      ASSERT_CMPINT64 (cursor_ids[1], ==, 2387509390608836392);

      mcd_rpc_message_destroy (rpc);
   }

   // Test that cursorIDs is being parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 8u);   // Truncate cursorID 1.
      data[20] = (uint8_t) (data[20] - 1u); // Set numberOfCursorIds to 1.

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (rpc), ==, 1);
      ASSERT_CMPINT64 (mcd_rpc_op_kill_cursors_get_cursor_ids (rpc)[0], ==, 1230066625199609624);
      mcd_rpc_message_destroy (rpc);

      data[20] = (uint8_t) (data[20] + 1u); // Restore numberOfCursorIds.
      data[0] = (uint8_t) (data[0] + 8u);   // Restore cursorID 1.
   }

   // Test that cursorIDs is being parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 16u);  // Exclude cursorIDs.
      data[20] = (uint8_t) (data[20] - 2u); // Set numberOfCursorIds to 0.

      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, NULL);
      ASSERT (rpc);
      ASSERT_CMPINT32 (mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (rpc), ==, 0);
      ASSERT (mcd_rpc_op_kill_cursors_get_cursor_ids (rpc) == NULL);
      mcd_rpc_message_destroy (rpc);

      data[20] = (uint8_t) (data[20] + 2u); // Restore numberOfCursorIds.
      data[0] = (uint8_t) (data[0] + 16u);  // Restore cursorID 1.
   }
}


static void
_test_from_data_invalid_decr (const char *file,
                              int line,
                              uint8_t *data,
                              size_t data_len,
                              bool expect_success,
                              size_t min,
                              size_t max,
                              size_t bytes_parsed_expected)
{
   ASSERT_WITH_MSG (min <= max, "%s:%d: min (%zu) should be less than or equal to max (%zu)", file, line, min, max);
   ASSERT_WITH_MSG (max < data_len, "%s:%d: max byte %zu exceeds input data length %zu", file, line, max, data_len);

   for (size_t i = min; i <= max; ++i) {
      data[i] = (uint8_t) (data[i] - 1u); // Set to original value - 1.

      {
         const void *data_end = NULL;
         mcd_rpc_message *rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

         if (expect_success) {
            ASSERT_WITH_MSG (rpc, "%s:%d: byte %zu: expected decrement to still succeed", file, line, i);
         } else {
            ASSERT_WITH_MSG (!rpc, "%s:%d: byte %zu: expected decrement to trigger failure", file, line, i);
         }

         const size_t bytes_parsed_actual = (size_t) ((const uint8_t *) data_end - data);

         ASSERT_WITH_MSG (bytes_parsed_expected == bytes_parsed_actual,
                          "%s:%d: byte %zu: expected decrement to cause "
                          "%zu bytes to be parsed, but parsed %zu bytes",
                          file,
                          line,
                          i,
                          bytes_parsed_expected,
                          bytes_parsed_actual);

         mcd_rpc_message_destroy (rpc);
      }

      data[i] = (uint8_t) (data[i] + 1u); // Revert to original value.
   }
}

static void
_test_from_data_invalid_incr (const char *file,
                              int line,
                              uint8_t *data,
                              size_t data_len,
                              bool expect_success,
                              size_t min,
                              size_t max,
                              size_t bytes_parsed_expected)
{
   ASSERT_WITH_MSG (max < data_len, "%s:%d: max byte %zu exceeds input data length %zu", file, line, max, data_len);

   for (size_t i = min; i <= max; ++i) {
      data[i] = (uint8_t) (data[i] + 1u); // Set to original value + 1.

      {
         const void *data_end = NULL;
         mcd_rpc_message *rpc = mcd_rpc_message_from_data (data, data_len, &data_end);

         if (expect_success) {
            ASSERT_WITH_MSG (rpc, "%s:%d: byte %zu: expected increment to still succeed", file, line, i);
         } else {
            ASSERT_WITH_MSG (!rpc, "%s:%d: byte %zu: expected increment to trigger failure", file, line, i);
         }

         const size_t bytes_parsed_actual = (size_t) ((const uint8_t *) data_end - data);

         ASSERT_WITH_MSG (bytes_parsed_expected == bytes_parsed_actual,
                          "%s:%d: byte %zu: expected increment to cause "
                          "%zu bytes to be parsed, but parsed %zu bytes",
                          file,
                          line,
                          i,
                          bytes_parsed_expected,
                          bytes_parsed_actual);

         mcd_rpc_message_destroy (rpc);
      }

      data[i] = (uint8_t) (data[i] - 1u); // Revert to original value.
   }
}

static void
_test_from_data_input_bounds (const uint8_t *data, size_t data_len)
{
   ASSERT_WITH_MSG (data[data_len] == 0xFF && data[data_len + 1u] == 0x00,
                    "expected input data to have extra bytes available for "
                    "boundary testing");

   // Reducing data length below messageLength should always trigger failure due
   // to insufficient bytes.
   for (size_t i = 0u; i < data_len; ++i) {
      const void *data_end = NULL;
      ASSERT_WITH_MSG (!mcd_rpc_message_from_data (data, i, &data_end),
                       "expected reduced data length of %zu to trigger "
                       "failure, but successfully parsed %zu bytes",
                       i,
                       (size_t) ((const uint8_t *) data_end - data));
   }

   // It is NOT an error for data length to be greater than messageLength.
   {
      const void *data_end = NULL;
      mcd_rpc_message *rpc = mcd_rpc_message_from_data (data, data_len + 1u, &data_end);
      ASSERT_CMPSIZE_T ((size_t) ((const uint8_t *) data_end - data), ==, data_len);
      ASSERT_WITH_MSG (rpc, "expected extra bytes to remain unparsed");
      ASSERT_CMPUINT (*(const uint8_t *) data_end, ==, 0xFF);
      mcd_rpc_message_destroy (rpc);
   }
}


#define EXPECT_DECR_FAILURE(min, max, end) \
   _test_from_data_invalid_decr (__FILE__, __LINE__, data, data_len, false, min, max, end)
#define EXPECT_DECR_SUCCESS(min, max, end) \
   _test_from_data_invalid_decr (__FILE__, __LINE__, data, data_len, true, min, max, end)
#define EXPECT_INCR_FAILURE(min, max, end) \
   _test_from_data_invalid_incr (__FILE__, __LINE__, data, data_len, false, min, max, end)
#define EXPECT_INCR_SUCCESS(min, max, end) \
   _test_from_data_invalid_incr (__FILE__, __LINE__, data, data_len, true, min, max, end)
#define EXPECT_DECR_IGNORED(min, max, end)
#define EXPECT_INCR_IGNORED(min, max, end)


static void
test_rpc_message_from_data_op_compressed_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_COMPRESSED, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_SUCCESS ( 0u,  0u, 44u); // messageLength (byte 0).
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): too large.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 45u); // requestID, responseTo.
   EXPECT_DECR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_DECR_SUCCESS (16u, 42u, 45u); // originalOpcode, uncompressedSize, compressorId, compressedMessage.

   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: too large.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 45u); // requestId, responseTo.
   EXPECT_INCR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_MSG.
   EXPECT_INCR_FAILURE (13u, 15u, 12u); // opCode (byte 1-3): invalid.
   EXPECT_INCR_SUCCESS (16u, 42u, 45u); // originalOpcode, uncompressedSize, compressorId, compressedMessage.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}


static void
test_rpc_message_from_data_op_msg_invalid_kind_0 (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_0, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 36u); // messageLength (byte 0): insufficient bytes to parse checksum.
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_COMPRESSED.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_DECR_FAILURE (16u, 16u, 36u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_NONE -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (17u, 17u, 16u); // flagBits (byte 1): invalid.
   EXPECT_DECR_SUCCESS (18u, 19u, 40u); // flagBits (bytes 2-3).
   EXPECT_DECR_FAILURE (20u, 20u, 20u); // Section 0 kind: invalid.
   EXPECT_DECR_FAILURE (21u, 21u, 35u); // Section 0 body length (byte 0): truncated body content -> invalid section 1 with duplicate kind 0.
   EXPECT_DECR_FAILURE (22u, 24u, 21u); // Section 0 body length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (25u, 35u, 40u); // Section 0 body content.
   EXPECT_DECR_SUCCESS (36u, 39u, 40u); // Checksum.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_INCR_FAILURE (16u, 16u, 36u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_MORE_TO_COME -> unexpected bytes remaining.
   EXPECT_INCR_FAILURE (17u, 17u, 16u); // flagBits (byte 1): invalid.
   EXPECT_INCR_FAILURE (12u, 15u, 12u); // opCode: invalid opCode.
   EXPECT_INCR_SUCCESS (18u, 19u, 40u); // flagBits (bytes 2-3).
   EXPECT_INCR_FAILURE (20u, 20u, 31u); // Section 0 kind: parse as document sequence -> invalid document 0 length (0x00000000).
   EXPECT_INCR_FAILURE (21u, 21u, 37u); // Section 0 body length (byte 0): extended body content -> insufficient bytes to parse checksum.
   EXPECT_INCR_FAILURE (22u, 24u, 21u); // Section 0 body length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (25u, 35u, 40u); // Section 0 body content.
   EXPECT_INCR_SUCCESS (36u, 39u, 40u); // Checksum.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_msg_invalid_kind_1_single (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_SINGLE, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 63u); // messageLength (byte 0): insufficient bytes to parse checksum.
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 67u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_COMPRESSED.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid opCode.
   EXPECT_DECR_FAILURE (16u, 16u, 63u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_NONE -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (17u, 17u, 16u); // flagBits (byte 1): invalid.
   EXPECT_DECR_SUCCESS (18u, 19u, 67u); // flagBits (bytes 2-3).
   EXPECT_DECR_FAILURE (20u, 20u, 20u); // Section 0 kind: invalid.
   EXPECT_DECR_FAILURE (21u, 21u, 35u); // Section 0 length (byte 0): truncated body content -> invalid section 1 with duplicate kind 0.
   EXPECT_DECR_FAILURE (22u, 24u, 21u); // Section 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (25u, 35u, 67u); // Section 0 body.
   EXPECT_DECR_FAILURE (36u, 36u, 36u); // Section 1 kind: parse as body -> invalid section 1 with duplicate kind 0.
   EXPECT_DECR_FAILURE (37u, 37u, 48u); // Section 1 length (byte 0): truncated document sequence content -> invalid document 0 length (15 bytes, 14 remaining).
   EXPECT_DECR_FAILURE (38u, 40u, 37u); // Section 1 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (41u, 46u, 67u); // Section 1 identifier (content).
   EXPECT_DECR_FAILURE (47u, 47u, 50u); // Section 1 identifier (terminator): extended identifier -> invalid document 0 length (0x6b100000).
   EXPECT_DECR_FAILURE (48u, 48u, 62u); // Section 1 document 0 length (byte 0): truncated document 0 content -> unexpected section bytes remaining.
   EXPECT_DECR_FAILURE (49u, 51u, 48u); // Section 1 document 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (52u, 62u, 67u); // Section 1 document 0 content.
   EXPECT_DECR_SUCCESS (63u, 66u, 67u); // Checksum.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 67u); // requestID, responseTo.
   EXPECT_INCR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_INCR_FAILURE (16u, 16u, 63u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_MORE_TO_COME -> unexpected bytes remaining.
   EXPECT_INCR_FAILURE (17u, 17u, 16u); // flagBits (byte 1): invalid.
   EXPECT_INCR_SUCCESS (18u, 19u, 67u); // flagBits (bytes 2-3).
   EXPECT_INCR_FAILURE (20u, 20u, 31u); // Section 0 kind: parse as document sequence -> invalid document 0 length (0x6b100000).
   EXPECT_INCR_FAILURE (21u, 21u, 37u); // Section 0 length (byte 0): extended body content -> invalid section 1 kind (0x1a).
   EXPECT_INCR_FAILURE (22u, 24u, 21u); // Section 0 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (25u, 35u, 67u); // Section 0 body.
   EXPECT_INCR_FAILURE (36u, 36u, 36u); // Section 1 kind: invalid.
   EXPECT_INCR_FAILURE (37u, 37u, 63u); // Section 1 length (byte 0): extended document sequence content -> insufficient bytes to parse checksum.
   EXPECT_INCR_FAILURE (38u, 40u, 37u); // Section 1 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (41u, 46u, 67u); // Section 1 identifier (content).
   EXPECT_INCR_FAILURE (47u, 47u, 50u); // Section 1 identifier (terminator): invalid document 0 length (0x6b100000).
   EXPECT_INCR_FAILURE (48u, 51u, 48u); // Section 1 document 0 length: invalid.
   EXPECT_DECR_SUCCESS (52u, 62u, 67u); // Section 1 document 0 content.
   EXPECT_INCR_SUCCESS (63u, 66u, 67u); // Checksum.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_msg_invalid_kind_1_multiple (void)
{
   uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_MULTIPLE, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE (  0u,   0u, 106u); // messageLength (byte 0): insufficient bytes to parse checksum.
   EXPECT_DECR_FAILURE (  1u,   3u,   0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (  4u,  11u, 110u); // requestID, responseTo.
   EXPECT_DECR_IGNORED ( 12u,  12u,  12u); // opCode (byte 0): parse as OP_COMPRESSED.
   EXPECT_DECR_FAILURE ( 13u,  15u,  12u); // opCode (bytes 1-3): invalid.
   EXPECT_DECR_FAILURE ( 16u,  16u, 106u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_NONE -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE ( 17u,  17u,  16u); // flagBits (byte 1): invalid.
   EXPECT_DECR_SUCCESS ( 18u,  19u, 110u); // flagBits (bytes 2-3).
   EXPECT_DECR_FAILURE ( 20u,  20u,  20u); // Section 0 kind: invalid.
   EXPECT_DECR_FAILURE ( 21u,  21u,  35u); // Section 0 length (byte 0): truncated body content -> invalid section 1 with duplicate kind 0.
   EXPECT_DECR_FAILURE ( 22u,  24u,  21u); // Section 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 25u,  35u, 110u); // Section 0 body.
   EXPECT_DECR_FAILURE ( 36u,  36u,  36u); // Section 1 kind: invalid section 1 with duplicate kind 0.
   EXPECT_DECR_FAILURE ( 37u,  37u,  47u); // Section 1 length (byte 0): truncated document sequence content -> invalid document 2 length (15 bytes, 14 remaining).
   EXPECT_DECR_FAILURE ( 38u,  40u,  37u); // Section 1 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 41u,  45u, 110u); // Section 1 identifier (content).
   EXPECT_DECR_FAILURE ( 46u,  46u,  49u); // Section 1 identifier (terminator): extended identifier -> invalid document 0 length (0x6b100000).
   EXPECT_DECR_FAILURE ( 47u,  47u,  61u); // Section 1 document 0 length (byte 0): truncated document 0 content -> invalid section 2 with duplicate kind 0.
   EXPECT_DECR_FAILURE ( 48u,  50u,  47u); // Section 1 document 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 51u,  61u, 110u); // Section 1 document 0 content.
   EXPECT_DECR_FAILURE ( 62u,  62u,  62u); // Section 2 kind: invalid section 2 with duplicate kind 0.
   EXPECT_DECR_FAILURE ( 63u,  63u,  90u); // Section 2 length (byte 0): truncated document sequence -> invalid document 1 length (16 bytes, 15 remaining).
   EXPECT_DECR_FAILURE ( 64u,  66u,  63u); // Section 2 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 67u,  72u, 110u); // Section 2 identifier (content).
   EXPECT_DECR_FAILURE ( 73u,  73u,  76u); // Section 2 identifier (terminator): extended identifier -> invalid document 0 length (0x69100000).
   EXPECT_DECR_FAILURE ( 74u,  74u,  89u); // Section 2 document 0 length (byte 0): truncated document 0 content -> invalid document 1 length (0x00001000).
   EXPECT_DECR_FAILURE ( 75u,  77u,  74u); // Section 2 document 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 78u,  89u, 110u); // Section 2 document 0 content.
   EXPECT_DECR_FAILURE ( 90u,  90u, 105u); // Section 2 document 1 length (byte 0): truncated document 1 content -> unexpected section bytes remaining.
   EXPECT_DECR_FAILURE ( 91u,  93u,  90u); // Section 2 document 1 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 94u, 105u, 110u); // Section 2 document 1 content.
   EXPECT_DECR_SUCCESS (106u, 109u, 110u); // Checksum.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE (  0u,   3u,   0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS (  4u,  11u, 110u); // requestID, responseTo.
   EXPECT_INCR_FAILURE ( 12u,  15u,  12u); // opCode: invalid.
   EXPECT_INCR_FAILURE ( 16u,  16u, 106u); // flagBits (byte 0): set to MONGOC_OP_MSG_FLAG_MORE_TO_COME -> unexpected bytes remaining.
   EXPECT_INCR_FAILURE ( 17u,  17u,  16u); // flagBits (byte 1): invalid.
   EXPECT_INCR_SUCCESS ( 18u,  19u, 110u); // flagBits (bytes 2-3).
   EXPECT_INCR_FAILURE ( 20u,  20u,  31u); // Section 0 kind: parse as document sequence -> invalid document 0 length (0x00000000).
   EXPECT_INCR_FAILURE ( 21u,  21u,  37u); // Section 0 length (byte 0): extended body content -> invalid section 1 kind (0x19).
   EXPECT_INCR_FAILURE ( 22u,  24u,  21u); // Section 0 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS ( 25u,  35u, 110u); // Section 0 body.
   EXPECT_INCR_FAILURE ( 36u,  36u,  36u); // Section 1 kind: invalid.
   EXPECT_INCR_FAILURE ( 37u,  37u,  62u); // Section 1 length (byte 0): extended document sequence content -> unexpected section bytes remaining.
   EXPECT_INCR_FAILURE ( 38u,  40u,  37u); // Section 1 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS ( 41u,  45u, 110u); // Section 1 identifier (content).
   EXPECT_INCR_FAILURE ( 46u,  46u,  49u); // Section 1 identifier (terminator): extended identifier -> invalid document 0 length (0x6b000000).
   EXPECT_INCR_FAILURE ( 47u,  50u,  47u); // Section 1 document 0 length: invalid.
   EXPECT_INCR_SUCCESS ( 51u,  61u, 110u); // Section 1 document 0 content.
   EXPECT_INCR_FAILURE ( 62u,  62u,  62u); // Section 2 kind: invalid.
   EXPECT_INCR_FAILURE ( 63u,  63u, 106u); // Section 2 length (byte 0): extended document sequence content -> unexpected section bytes remaining.
   EXPECT_INCR_FAILURE ( 64u,  66u,  63u); // Section 2 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS ( 67u,  72u, 110u); // Section 2 identifier (content).
   EXPECT_INCR_FAILURE ( 73u,  73u,  76u); // Section 2 identifier (terminator): extended identifier -> invalid document 0 length (0x69100000).
   EXPECT_INCR_FAILURE ( 74u,  74u,  91u); // Section 2 document 0 length (byte 0): extended document 0 content -> invalid document 1 length (0x10000000).
   EXPECT_INCR_FAILURE ( 75u,  77u,  74u); // Section 2 document 0 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS ( 78u,  89u, 110u); // Section 2 document 0 content.
   EXPECT_INCR_FAILURE ( 90u,  93u,  90u); // Section 2 document 1 length: invalid.
   EXPECT_INCR_SUCCESS ( 94u, 105u, 110u); // Section 2 document 1 content.
   EXPECT_INCR_SUCCESS (106u, 109u, 110u); // Checksum.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_msg_invalid (void)
{
   test_rpc_message_from_data_op_msg_invalid_kind_0 ();
   test_rpc_message_from_data_op_msg_invalid_kind_1_single ();
   test_rpc_message_from_data_op_msg_invalid_kind_1_multiple ();
}

static void
test_rpc_message_from_data_op_reply_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_REPLY, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 52u); // messageLength (byte 0): truncated document 1 content -> invalid document 1 length (16 bytes, 15 remaining).
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 68u); // requestID, responseTo.
   EXPECT_DECR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_DECR_SUCCESS (16u, 31u, 68u); // responseFlags, cursorID, startingFrom.
   EXPECT_DECR_FAILURE (32u, 32u, 52u); // numberReturned (byte 0): set to 1 -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (33u, 34u, 68u); // numberReturned (bytes 1-2): insufficient bytes to parse document 2.
   EXPECT_DECR_FAILURE (35u, 35u, 32u); // numberReturned (bytes 3): invalid.
   EXPECT_DECR_FAILURE (36u, 36u, 51u); // Document 0 length (byte 0): truncated document 0 content -> invalid document 1 length (0x00001000).
   EXPECT_DECR_FAILURE (37u, 39u, 36u); // Document 0 length (bytes 1-3): invalid length.
   EXPECT_DECR_SUCCESS (40u, 51u, 68u); // Document 0 content.
   EXPECT_DECR_FAILURE (52u, 52u, 67u); // Document 1 length (byte 0): truncated document 1 content -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (53u, 55u, 52u); // Document 1 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (56u, 67u, 68u); // Document 1 content.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 68u); // requestID, responseTo.
   EXPECT_INCR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_INCR_SUCCESS (16u, 31u, 68u); // responseFlags, cursorID, startingFrom.
   EXPECT_INCR_FAILURE (32u, 35u, 68u); // numberReturned: insufficient bytes to parse document 2.
   EXPECT_INCR_FAILURE (36u, 36u, 53u); // Document 0 length (byte 0): extended document 0 content -> invalid document 1 length (0x10000000).
   EXPECT_INCR_FAILURE (37u, 39u, 36u); // Document 0 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (40u, 51u, 68u); // Document 0 content.
   EXPECT_INCR_FAILURE (52u, 55u, 52u); // Document 1 length: invalid.
   EXPECT_INCR_SUCCESS (56u, 67u, 68u); // Document 1 content.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_update_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_UPDATE, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 48u); // messageLength (byte 0): truncated update document content -> invalid update document length (14 bytes, 13 remaining).
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 62u); // requestID, responseTo.
   EXPECT_DECR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_DECR_SUCCESS (20u, 26u, 62u); // fullCollectionName (content).
   EXPECT_DECR_FAILURE (27u, 27u, 29u); // fullCollectionName (terminator): extended fullCollectionName -> invalid flags (0x10000000).
   EXPECT_DECR_FAILURE (28u, 31u, 28u); // flags: invalid.
   EXPECT_DECR_FAILURE (32u, 32u, 47u); // selector document length (byte 0): truncated selector document content -> invalid update document length (0x00000e00).
   EXPECT_DECR_FAILURE (33u, 35u, 32u); // selector document length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (36u, 47u, 62u); // selector document content.
   EXPECT_DECR_FAILURE (48u, 48u, 61u); // update document length (byte 0): truncated update document content -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (49u, 51u, 48u); // update document length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (52u, 61u, 62u); // update document content.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 62u); // requestID, responseTo.
   EXPECT_INCR_IGNORED (12u, 12u, 12u); // opCode: parse as OP_INSERT.
   EXPECT_INCR_FAILURE (13u, 15u, 12u); // opCode: invalid.
   EXPECT_INCR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_INCR_SUCCESS (20u, 26u, 62u); // fullCollectionName (content).
   EXPECT_INCR_FAILURE (27u, 27u, 29u); // fullCollectionName (terminator): extended fullCollectionName -> invalid flags (0x10000000).
   EXPECT_INCR_SUCCESS (28u, 28u, 62u); // flags (byte 0): set to MONGOC_OP_UPDATE_FLAG_UPSERT.
   EXPECT_INCR_FAILURE (29u, 31u, 28u); // flags (bytes 1-3): invalid.
   EXPECT_INCR_FAILURE (32u, 32u, 49u); // selector document length (byte 0): extended selector document -> invalid update document length (0x08000000).
   EXPECT_INCR_FAILURE (33u, 35u, 32u); // selector document length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (36u, 47u, 62u); // selector document content.
   EXPECT_INCR_FAILURE (48u, 51u, 48u); // update document length: invalid length.
   EXPECT_INCR_SUCCESS (52u, 61u, 62u); // update document content.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_insert_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_INSERT, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 44u); // messageLength (byte 0): truncated document 1 content -> invalid document 1 length (16 bytes, 15 remaining).
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 60u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode: parse as OP_UPDATE.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode: invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // flags: invalid.
   EXPECT_DECR_SUCCESS (20u, 26u, 60u); // fullCollectionName (content).
   EXPECT_DECR_FAILURE (27u, 27u, 30u); // fullCollectionName (terminator): extended fullCollectionName -> invalid document 0 length (0x69100000).
   EXPECT_DECR_FAILURE (28u, 28u, 43u); // Document 0 length (byte 0): truncated document 0 content -> invalid document 1 length (0x00001000).
   EXPECT_DECR_FAILURE (29u, 31u, 28u); // Document 0 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (32u, 43u, 60u); // Document 0 content.
   EXPECT_DECR_FAILURE (44u, 44u, 59u); // Document 1 length (byte 0): truncated document 1 content -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (45u, 47u, 44u); // Document 1 length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (48u, 59u, 60u); // Document 1 content.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 60u); // requestID, responseTo.
   EXPECT_INCR_FAILURE (12u, 15u, 12u); // opCode: invalid opCode.
   EXPECT_INCR_SUCCESS (16u, 16u, 60u); // flags (byte 0): set to MONGOC_OP_INSERT_FLAG_CONTINUE_ON_ERROR.
   EXPECT_INCR_FAILURE (17u, 19u, 16u); // flags (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (20u, 26u, 60u); // fullCollectionName (content).
   EXPECT_INCR_FAILURE (27u, 27u, 30u); // fullCollectionName (terminator): extended fullCollectionName -> invalid document 0 length (0x69100000).
   EXPECT_INCR_FAILURE (28u, 28u, 45u); // Document 0 length (byte 0): extended document 0 content -> invalid document 1 length (0x10000000).
   EXPECT_INCR_FAILURE (29u, 31u, 28u); // Document 0 length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (32u, 43u, 60u); // Document 0 content.
   EXPECT_INCR_FAILURE (44u, 47u, 44u); // Document 1 length: invalid.
   EXPECT_INCR_SUCCESS (48u, 59u, 60u); // Document 1 content.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);

   // Test that documents are parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 32u); // Exclude both documents.

      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);
      const size_t parsed_len = (size_t) ((const uint8_t *) data_end - data);
      const size_t expected_len = 28u;

      ASSERT_WITH_MSG (!rpc, "OP_INSERT requires at least one document");
      ASSERT_WITH_MSG (parsed_len == expected_len,
                       "expected %zu bytes to be parsed before error, but parsed %zu bytes",
                       expected_len,
                       parsed_len);

      mcd_rpc_message_destroy (rpc);

      data[0] = (uint8_t) (data[0] + 32u); // Restore documents.
   }
}

static void
test_rpc_message_from_data_op_query_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_QUERY, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 49u); // messageLength (byte 0): truncated returnFieldsSelector document content -> invalid returnFieldsSelector document length (16 bytes, 15 remaining).
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 65u); // requestID, responseTo.
   EXPECT_DECR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // flags: invalid.
   EXPECT_DECR_SUCCESS (20u, 26u, 65u); // fullCollectionName (content).
   EXPECT_DECR_FAILURE (27u, 27u, 37u); // fullCollectionName (terminator): extended fullCollectionName -> invalid query document length (0x08000000).
   EXPECT_DECR_SUCCESS (28u, 35u, 65u); // numberToSkip, numberToReturn.
   EXPECT_DECR_FAILURE (36u, 36u, 48u); // query document length (byte 0): truncated query document content -> invalid returnFieldsSelector document length (0x00001000).
   EXPECT_DECR_FAILURE (37u, 39u, 36u); // query document length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (40u, 48u, 65u); // query document content.
   EXPECT_DECR_FAILURE (49u, 49u, 64u); // returnFieldsSelector document length (byte 0): truncated returnFieldsSelector document content -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (50u, 52u, 49u); // returnFieldsSelector document length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (53u, 64u, 65u); // returnFieldsSelector document content.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 65u); // requestID, responseTo.
   EXPECT_INCR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_GET_MORE.
   EXPECT_INCR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_INCR_FAILURE (16u, 19u, 16u); // flags (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (20u, 26u, 65u); // fullCollectionName (content).
   EXPECT_INCR_FAILURE (27u, 27u, 37u); // fullCollectionName (terminator): extended fullCollectionName -> invalid query document length (0x08000000).
   EXPECT_INCR_SUCCESS (28u, 35u, 65u); // numberToSkip, numberToReturn.
   EXPECT_INCR_FAILURE (36u, 36u, 50u); // query document length (byte 0): extended query document content -> invalid returnFieldsSelector document length (0x08000000).
   EXPECT_INCR_FAILURE (37u, 39u, 36u); // query document length (bytes 1-3): invalid.
   EXPECT_INCR_SUCCESS (40u, 48u, 65u); // query document content.
   EXPECT_INCR_FAILURE (49u, 52u, 49u); // returnFieldsSelector document length: invalid.
   EXPECT_INCR_SUCCESS (53u, 64u, 65u); // returnFieldsSelector document content.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);

   // Test that the query document is parsed correctly.
   {
      data[0] = (uint8_t) (data[0] - 29u); // Exclude both query and
                                           // returnFieldsSelector documents.

      const void *data_end = NULL;
      mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, data_len, &data_end);
      const size_t parsed_len = (size_t) ((const uint8_t *) data_end - data);
      const size_t expected_len = 36u;

      ASSERT_WITH_MSG (!rpc, "OP_QUERY requires a query document");
      ASSERT_WITH_MSG (parsed_len == expected_len,
                       "expected %zu bytes to be parsed before error, but parsed %zu bytes",
                       expected_len,
                       parsed_len);

      mcd_rpc_message_destroy (rpc);

      data[0] = (uint8_t) (data[0] + 29u); // Restore both query and
                                           // returnFieldsSelector documents.
   }
}

static void
test_rpc_message_from_data_op_get_more_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_GET_MORE, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 32u); // messageLength (byte 0): insufficient bytes to parse cursorID.
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_QUERY.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_DECR_SUCCESS (20u, 26u, 40u); // fullCollectionName (content).
   EXPECT_DECR_FAILURE (27u, 27u, 33u); // fullCollectionName (terminator): extended fullCollectionName -> insufficient bytes to parse cursorID.
   EXPECT_DECR_SUCCESS (28u, 39u, 40u); // fullCollectionName, numberToReturn, cursorID.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_INCR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_DELETE.
   EXPECT_INCR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_INCR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_INCR_SUCCESS (20u, 26u, 40u); // fullCollectionName (content).
   EXPECT_INCR_FAILURE (27u, 27u, 33u); // fullCollectionName (terminator): extended fullCollectionName -> insufficient bytes to parse cursorID.
   EXPECT_INCR_SUCCESS (28u, 39u, 40u); // fullCollectionName, numberToReturn, cursorID.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_delete_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_DELETE, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 32u); // messageLength (byte 0): insufficient bytes to parse cursorID.
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 48u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_GET_MORE.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_DECR_SUCCESS (20u, 26u, 48u); // fullCollectionName (content).
   EXPECT_DECR_FAILURE (27u, 27u, 29u); // fullCollectionName (terminator): extended fullCollectionName -> invalid flags (0x10000000).
   EXPECT_DECR_FAILURE (28u, 31u, 28u); // flags: invalid.
   EXPECT_DECR_FAILURE (32u, 32u, 47u); // selector document length (byte 0): truncated selector document content -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (33u, 35u, 32u); // selector document length (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (36u, 47u, 48u); // selector document content.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 48u); // requestID, responseTo.
   EXPECT_INCR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_KILL_CURSORS.
   EXPECT_INCR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_INCR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_INCR_SUCCESS (20u, 26u, 48u); // fullCollectionName (content).
   EXPECT_INCR_FAILURE (27u, 27u, 29u); // fullCollectionName (terminator): extended fullCollectionName -> invalid flags (0x10000000).
   EXPECT_INCR_SUCCESS (28u, 28u, 48u); // flags (byte 0): set to MONGOC_OP_DELETE_FLAG_SINGLE_REMOVE.
   EXPECT_INCR_FAILURE (29u, 31u, 28u); // flags (bytes 1-3): invalid.
   EXPECT_INCR_FAILURE (32u, 35u, 32u); // selector document length: invalid.
   EXPECT_INCR_SUCCESS (36u, 47u, 48u); // selector document content.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}

static void
test_rpc_message_from_data_op_kill_cursors_invalid (void)
{
   uint8_t data[] = {TEST_DATA_OP_KILL_CURSORS, 0xFF, 0x00};
   const size_t data_len = sizeof (data) - 2u; // Exclude the extra bytes.

   // clang-format off
   EXPECT_DECR_FAILURE ( 0u,  0u, 20u); // messageLength (byte 0): invalid numberOfCursorIds.
   EXPECT_DECR_FAILURE ( 1u,  3u,  0u); // messageLength (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_DECR_IGNORED (12u, 12u, 12u); // opCode (byte 0): parse as OP_DELETE.
   EXPECT_DECR_FAILURE (13u, 15u, 12u); // opCode (bytes 1-3): invalid.
   EXPECT_DECR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_DECR_FAILURE (20u, 20u, 32u); // numberOfCursorIds (byte 0): truncated cursorIDs -> unexpected bytes remaining.
   EXPECT_DECR_FAILURE (21u, 23u, 20u); // numberOfCursorIds (bytes 1-3): invalid.
   EXPECT_DECR_SUCCESS (24u, 39u, 40u); // cursorIDs.
   // clang-format on

   // clang-format off
   EXPECT_INCR_FAILURE ( 0u,  3u,  0u); // messageLength: invalid.
   EXPECT_INCR_SUCCESS ( 4u, 11u, 40u); // requestID, responseTo.
   EXPECT_INCR_FAILURE (12u, 15u, 12u); // opCode: invalid.
   EXPECT_INCR_FAILURE (16u, 19u, 16u); // ZERO: invalid.
   EXPECT_INCR_FAILURE (20u, 23u, 20u); // numberOfCursorIds: invalid.
   EXPECT_INCR_SUCCESS (24u, 39u, 40u); // cursorIDs.
   // clang-format on

   _test_from_data_input_bounds (data, data_len);
}


#define ASSERT_IOVEC_VALUE(index, expected, type, raw_type, from_le, spec)                                             \
   if (1) {                                                                                                            \
      const type _expected = expected;                                                                                 \
      const mongoc_iovec_t iovec = iovecs[index];                                                                      \
      const size_t len = sizeof (type);                                                                                \
      ASSERT_WITH_MSG (iovec.iov_len == sizeof (type), "expected iov_len to be %zu, but got %zu", len, iovec.iov_len); \
      raw_type storage;                                                                                                \
      memcpy (&storage, iovec.iov_base, sizeof (type));                                                                \
      storage = from_le (storage);                                                                                     \
      type value;                                                                                                      \
      memcpy (&value, &storage, sizeof (type));                                                                        \
      ASSERT_WITH_MSG (value == _expected,                                                                             \
                       "expected iov_base to point to %s with value %" spec ", but got %" spec,                        \
                       #type,                                                                                          \
                       _expected,                                                                                      \
                       value);                                                                                         \
   } else                                                                                                              \
      (void) 0

#define ASSERT_IOVEC_UINT8(index, expected) ASSERT_IOVEC_VALUE (index, expected, uint8_t, uint8_t, (uint8_t), PRIu8)
#define ASSERT_IOVEC_INT32(index, expected) \
   ASSERT_IOVEC_VALUE (index, expected, int32_t, uint32_t, BSON_UINT32_FROM_LE, PRId32)
#define ASSERT_IOVEC_UINT32(index, expected) \
   ASSERT_IOVEC_VALUE (index, expected, uint32_t, uint32_t, BSON_UINT32_FROM_LE, PRIu32)
#define ASSERT_IOVEC_INT64(index, expected) \
   ASSERT_IOVEC_VALUE (index, expected, int64_t, uint64_t, BSON_UINT64_FROM_LE, PRId64)

#define ASSERT_IOVEC_BYTES(index, expected_base_index, expected_len)                         \
   if (1) {                                                                                  \
      const mongoc_iovec_t iovec = iovecs[index];                                            \
      ASSERT_WITH_MSG (iovec.iov_len == expected_len,                                        \
                       "expected iov_len to be %zu, but got %zu",                            \
                       (size_t) expected_len,                                                \
                       iovec.iov_len);                                                       \
      ASSERT_WITH_MSG ((const uint8_t *) iovec.iov_base == (data + expected_base_index),     \
                       "expected iov_base to point to byte %zu (%p), but got byte %zu (%p)", \
                       (size_t) (expected_base_index),                                       \
                       (void *) (data + expected_base_index),                                \
                       (size_t) ((const uint8_t *) iovec.iov_base - data),                   \
                       iovec.iov_base);                                                      \
   } else                                                                                    \
      (void) 0


static void
test_rpc_message_to_iovecs_op_compressed (void)
{
   const uint8_t data[] = {TEST_DATA_OP_COMPRESSED};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_IOVEC_INT32 (0, 45);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2012);     // opCode
   ASSERT_IOVEC_INT32 (4, 2013);     // originalOpcode
   ASSERT_IOVEC_INT32 (5, 20);       // uncompressedSize
   ASSERT_IOVEC_UINT8 (6, 0u);       // compressorId
   ASSERT_IOVEC_BYTES (7, 25u, 20u); // compressedMessage

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_msg_kind_0 (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_0};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_IOVEC_INT32 (0, 40);         // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060);   // requestID
   ASSERT_IOVEC_INT32 (2, 84281096);   // responseTo
   ASSERT_IOVEC_INT32 (3, 2013);       // opCode
   ASSERT_IOVEC_INT32 (4, 1);          // flagBits
   ASSERT_IOVEC_UINT8 (5, 0);          // Section 0 Kind
   ASSERT_IOVEC_BYTES (6, 21u, 15u);   // Section 0 Body
   ASSERT_IOVEC_UINT32 (7, 287454020); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_msg_kind_1_single (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_SINGLE};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 12u);
   ASSERT_IOVEC_INT32 (0, 67);          // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060);    // requestID
   ASSERT_IOVEC_INT32 (2, 84281096);    // responseTo
   ASSERT_IOVEC_INT32 (3, 2013);        // opCode
   ASSERT_IOVEC_INT32 (4, 1);           // flagBits
   ASSERT_IOVEC_UINT8 (5, 0);           // Section 0 Kind
   ASSERT_IOVEC_BYTES (6, 21u, 15u);    // Section 0 Body
   ASSERT_IOVEC_UINT8 (7, 1);           // Section 1 Kind
   ASSERT_IOVEC_INT32 (8, 26);          // Section 1 Length
   ASSERT_IOVEC_BYTES (9, 41u, 7u);     // Section 1 Identifier
   ASSERT_IOVEC_BYTES (10, 48u, 15u);   // Section 1 Documents
   ASSERT_IOVEC_UINT32 (11, 287454020); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_msg_kind_1_multiple (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_MULTIPLE};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 16u);
   ASSERT_IOVEC_INT32 (0, 110);         // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060);    // requestID
   ASSERT_IOVEC_INT32 (2, 84281096);    // responseTo
   ASSERT_IOVEC_INT32 (3, 2013);        // opCode
   ASSERT_IOVEC_INT32 (4, 1);           // flagBits
   ASSERT_IOVEC_UINT8 (5, 0);           // Section 0 Kind
   ASSERT_IOVEC_BYTES (6, 21u, 15u);    // Section 0 Body
   ASSERT_IOVEC_UINT8 (7, 1);           // Section 1 Kind
   ASSERT_IOVEC_INT32 (8, 25);          // Section 1 Length
   ASSERT_IOVEC_BYTES (9, 41u, 6u);     // Section 1 Identifier
   ASSERT_IOVEC_BYTES (10, 47u, 15u);   // Section 1 Documents
   ASSERT_IOVEC_UINT8 (11, 1);          // Section 2 Kind
   ASSERT_IOVEC_INT32 (12, 43);         // Section 2 Length
   ASSERT_IOVEC_BYTES (13, 67u, 7u);    // Section 2 Identifier
   ASSERT_IOVEC_BYTES (14, 74u, 32u);   // Section 2 Documents
   ASSERT_IOVEC_UINT32 (15, 287454020); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_msg (void)
{
   test_rpc_message_to_iovecs_op_msg_kind_0 ();
   test_rpc_message_to_iovecs_op_msg_kind_1_single ();
   test_rpc_message_to_iovecs_op_msg_kind_1_multiple ();
}

static void
test_rpc_message_to_iovecs_op_reply (void)
{
   const uint8_t data[] = {TEST_DATA_OP_REPLY};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 9u);
   ASSERT_IOVEC_INT32 (0, 68);                            // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060);                      // requestID
   ASSERT_IOVEC_INT32 (2, 84281096);                      // responseTo
   ASSERT_IOVEC_INT32 (3, 1);                             // opCode
   ASSERT_IOVEC_INT32 (4, 0);                             // responseFlags
   ASSERT_IOVEC_INT64 (5, INT64_C (1234605616436508552)); // cursorID
   ASSERT_IOVEC_INT32 (6, 0);                             // startingFrom
   ASSERT_IOVEC_INT32 (7, 2);                             // numberReturned
   ASSERT_IOVEC_BYTES (8, 36u, 32u);                      // documents

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_update (void)
{
   const uint8_t data[] = {TEST_DATA_OP_UPDATE};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 9u);
   ASSERT_IOVEC_INT32 (0, 62);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2001);     // opCode
   ASSERT_IOVEC_INT32 (4, 0);        // ZERO
   ASSERT_IOVEC_BYTES (5, 20u, 8u);  // fullCollectionName
   ASSERT_IOVEC_INT32 (6, 0);        // flags
   ASSERT_IOVEC_BYTES (7, 32u, 16u); // selector
   ASSERT_IOVEC_BYTES (8, 48u, 14u); // update

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_insert (void)
{
   const uint8_t data[] = {TEST_DATA_OP_INSERT};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 7u);
   ASSERT_IOVEC_INT32 (0, 60);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2002);     // opCode
   ASSERT_IOVEC_INT32 (4, 0);        // flags
   ASSERT_IOVEC_BYTES (5, 20u, 8u);  // fullCollectionName
   ASSERT_IOVEC_BYTES (6, 28u, 32u); // documents

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_query (void)
{
   const uint8_t data[] = {TEST_DATA_OP_QUERY};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 10u);
   ASSERT_IOVEC_INT32 (0, 65);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2004);     // opCode
   ASSERT_IOVEC_INT32 (4, 0);        // flags
   ASSERT_IOVEC_BYTES (5, 20u, 8u);  // fullCollectionName
   ASSERT_IOVEC_INT32 (6, 0);        // numberToSkip
   ASSERT_IOVEC_INT32 (7, 0);        // numberToReturn
   ASSERT_IOVEC_BYTES (8, 36u, 13u); // query
   ASSERT_IOVEC_BYTES (9, 49u, 16u); // returnFieldsSelector

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_get_more (void)
{
   const uint8_t data[] = {TEST_DATA_OP_GET_MORE};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_IOVEC_INT32 (0, 40);                            // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060);                      // requestID
   ASSERT_IOVEC_INT32 (2, 84281096);                      // responseTo
   ASSERT_IOVEC_INT32 (3, 2005);                          // opCode
   ASSERT_IOVEC_INT32 (4, 0);                             // ZERO
   ASSERT_IOVEC_BYTES (5, 20u, 8u);                       // fullCollectionName
   ASSERT_IOVEC_INT32 (6, 0);                             // numberToReturn
   ASSERT_IOVEC_INT64 (7, INT64_C (1234605616436508552)); // cursorID

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_delete (void)
{
   const uint8_t data[] = {TEST_DATA_OP_DELETE};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_IOVEC_INT32 (0, 48);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2006);     // opCode
   ASSERT_IOVEC_INT32 (4, 0);        // ZERO
   ASSERT_IOVEC_BYTES (5, 20u, 8u);  // fullCollectionName
   ASSERT_IOVEC_INT32 (6, 0);        // flags
   ASSERT_IOVEC_BYTES (7, 32u, 16u); // selector

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}

static void
test_rpc_message_to_iovecs_op_kill_cursors (void)
{
   const uint8_t data[] = {TEST_DATA_OP_KILL_CURSORS};

   mcd_rpc_message *const rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, 7u);
   ASSERT_IOVEC_INT32 (0, 40);       // messageLength
   ASSERT_IOVEC_INT32 (1, 16909060); // requestID
   ASSERT_IOVEC_INT32 (2, 84281096); // responseTo
   ASSERT_IOVEC_INT32 (3, 2007);     // opCode
   ASSERT_IOVEC_INT32 (4, 0);        // ZERO
   ASSERT_IOVEC_INT32 (5, 2);        // numberOfCursorIds

   // The cursorIDs iovec does not point to original data due to endian
   // conversion requirements. Assert values individually.
   {
      const mongoc_iovec_t *const iovec = iovecs + 6;

      ASSERT_CMPSIZE_T (iovec->iov_len, ==, 16u);

      const int64_t *const cursor_ids = (const int64_t *) iovec->iov_base;

      const int64_t cursor_id_0 = _int64_from_le (cursor_ids + 0);
      const int64_t cursor_id_1 = _int64_from_le (cursor_ids + 1);

      ASSERT_CMPINT64 (cursor_id_0, ==, 1230066625199609624);
      ASSERT_CMPINT64 (cursor_id_1, ==, 2387509390608836392);
   }

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);
}


#define ASSERT_CMPIOVEC_VALUE(index, type, raw_type, from_le, spec)                                                 \
   if (1) {                                                                                                         \
      const mongoc_iovec_t _actual = iovecs[index];                                                                 \
      const mongoc_iovec_t _expected = expected_iovecs[index];                                                      \
      ASSERT_WITH_MSG (_expected.iov_len == sizeof (type), "expected iov_len does not match expected type length"); \
      ASSERT_WITH_MSG (_actual.iov_len == _expected.iov_len,                                                        \
                       "expected iov_len to be %zu, but got %zu",                                                   \
                       _expected.iov_len,                                                                           \
                       _actual.iov_len);                                                                            \
      raw_type storage;                                                                                             \
      type actual_value;                                                                                            \
      type expected_value;                                                                                          \
      memcpy (&storage, _actual.iov_base, sizeof (type));                                                           \
      storage = from_le (storage);                                                                                  \
      memcpy (&actual_value, &storage, sizeof (type));                                                              \
      memcpy (&storage, _expected.iov_base, sizeof (type));                                                         \
      storage = from_le (storage);                                                                                  \
      memcpy (&expected_value, &storage, sizeof (type));                                                            \
      ASSERT_WITH_MSG (actual_value == expected_value,                                                              \
                       "expected iov_base to point to %s with value %" spec ", but got %" spec,                     \
                       #type,                                                                                       \
                       expected_value,                                                                              \
                       actual_value);                                                                               \
   } else                                                                                                           \
      (void) 0

#define ASSERT_CMPIOVEC_UINT8(index) ASSERT_CMPIOVEC_VALUE (index, uint8_t, uint8_t, (uint8_t), PRIu8)
#define ASSERT_CMPIOVEC_INT32(index) ASSERT_CMPIOVEC_VALUE (index, int32_t, uint32_t, BSON_UINT32_FROM_LE, PRId32)
#define ASSERT_CMPIOVEC_UINT32(index) ASSERT_CMPIOVEC_VALUE (index, uint32_t, uint32_t, BSON_UINT32_FROM_LE, PRIu32)
#define ASSERT_CMPIOVEC_INT64(index) ASSERT_CMPIOVEC_VALUE (index, int64_t, uint64_t, BSON_UINT64_FROM_LE, PRId64)

#define ASSERT_CMPIOVEC_BYTES(index)                                                         \
   if (1) {                                                                                  \
      const mongoc_iovec_t _actual = iovecs[index];                                          \
      const mongoc_iovec_t _expected = expected_iovecs[index];                               \
      ASSERT_WITH_MSG (_actual.iov_len == _expected.iov_len,                                 \
                       "expected iov_len to be %zu, but got %zu",                            \
                       _expected.iov_len,                                                    \
                       _actual.iov_len);                                                     \
      ASSERT_WITH_MSG (_actual.iov_base == _expected.iov_base,                               \
                       "expected iov_base to point to byte %zu (%p), but got byte %zu (%p)", \
                       (size_t) ((const uint8_t *) _expected.iov_base - data),               \
                       _expected.iov_base,                                                   \
                       (size_t) ((const uint8_t *) _actual.iov_base - data),                 \
                       _actual.iov_base);                                                    \
   } else                                                                                    \
      (void) 0


static void
test_rpc_message_setters_op_compressed (void)
{
   const uint8_t data[] = {TEST_DATA_OP_COMPRESSED};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 45));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_COMPRESSED));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_compressed_set_original_opcode (rpc, MONGOC_OP_CODE_MSG));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_compressed_set_uncompressed_size (rpc, 20));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_compressed_set_compressor_id (rpc, 0));
   ASSERT_CMPINT32 (20, ==, mcd_rpc_op_compressed_set_compressed_message (rpc, data + 25, 20u));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // originalOpcode
   ASSERT_CMPIOVEC_INT32 (5); // uncompressedSize
   ASSERT_CMPIOVEC_UINT8 (6); // compressorId
   ASSERT_CMPIOVEC_BYTES (7); // compressedMessage

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_msg_kind_0 (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_0};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 40));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG));
   mcd_rpc_op_msg_set_sections_count (rpc, 1u);
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_flag_bits (rpc, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0));
   ASSERT_CMPINT32 (15, ==, mcd_rpc_op_msg_section_set_body (rpc, 0u, data + 21));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_checksum (rpc, 287454020));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_CMPIOVEC_INT32 (0);  // messageLength
   ASSERT_CMPIOVEC_INT32 (1);  // requestID
   ASSERT_CMPIOVEC_INT32 (2);  // responseTo
   ASSERT_CMPIOVEC_INT32 (3);  // opCode
   ASSERT_CMPIOVEC_INT32 (4);  // flagBits
   ASSERT_CMPIOVEC_UINT8 (5);  // Section 0 Kind
   ASSERT_CMPIOVEC_BYTES (6);  // Section 0 Body
   ASSERT_CMPIOVEC_UINT32 (7); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_msg_kind_1_single (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_SINGLE};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 67));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG));
   mcd_rpc_op_msg_set_sections_count (rpc, 2u);
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_flag_bits (rpc, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0));
   ASSERT_CMPINT32 (15, ==, mcd_rpc_op_msg_section_set_body (rpc, 0u, data + 21));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 1u, 1));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_section_set_length (rpc, 1u, 26u));
   ASSERT_CMPINT32 ( 7, ==, mcd_rpc_op_msg_section_set_identifier (rpc, 1u, (const char *) (data + 41)));
   ASSERT_CMPINT32 (15, ==, mcd_rpc_op_msg_section_set_document_sequence (rpc, 1u, data + 48, 15u));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_checksum (rpc, 287454020));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 12u);
   ASSERT_CMPIOVEC_INT32 (0);   // messageLength
   ASSERT_CMPIOVEC_INT32 (1);   // requestID
   ASSERT_CMPIOVEC_INT32 (2);   // responseTo
   ASSERT_CMPIOVEC_INT32 (3);   // opCode
   ASSERT_CMPIOVEC_INT32 (4);   // flagBits
   ASSERT_CMPIOVEC_UINT8 (5);   // Section 0 Kind
   ASSERT_CMPIOVEC_BYTES (6);   // Section 0 Body
   ASSERT_CMPIOVEC_UINT8 (7);   // Section 1 Kind
   ASSERT_CMPIOVEC_INT32 (8);   // Section 1 Length
   ASSERT_CMPIOVEC_BYTES (9);   // Section 1 Identifier
   ASSERT_CMPIOVEC_BYTES (10);  // Section 1 Documents
   ASSERT_CMPIOVEC_UINT32 (11); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_msg_kind_1_multiple (void)
{
   const uint8_t data[] = {TEST_DATA_OP_MSG_KIND_1_MULTIPLE};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 110));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG));
   mcd_rpc_op_msg_set_sections_count (rpc, 3u);
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_flag_bits (rpc, MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0));
   ASSERT_CMPINT32 (15, ==, mcd_rpc_op_msg_section_set_body (rpc, 0u, data + 21));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 1u, 1));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_section_set_length (rpc, 1u, 25u));
   ASSERT_CMPINT32 ( 6, ==, mcd_rpc_op_msg_section_set_identifier (rpc, 1u, (const char *) (data + 41)));
   ASSERT_CMPINT32 (15, ==, mcd_rpc_op_msg_section_set_document_sequence (rpc, 1u, data + 47, 15u));
   ASSERT_CMPINT32 ( 1, ==, mcd_rpc_op_msg_section_set_kind (rpc, 2u, 1));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_section_set_length (rpc, 2u, 43u));
   ASSERT_CMPINT32 ( 7, ==, mcd_rpc_op_msg_section_set_identifier (rpc, 2u, (const char *) (data + 67)));
   ASSERT_CMPINT32 (32, ==, mcd_rpc_op_msg_section_set_document_sequence (rpc, 2u, data + 74, 32u));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_msg_set_checksum (rpc, 287454020));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 16u);
   ASSERT_CMPIOVEC_INT32 (0);   // messageLength
   ASSERT_CMPIOVEC_INT32 (1);   // requestID
   ASSERT_CMPIOVEC_INT32 (2);   // responseTo
   ASSERT_CMPIOVEC_INT32 (3);   // opCode
   ASSERT_CMPIOVEC_INT32 (4);   // flagBits
   ASSERT_CMPIOVEC_UINT8 (5);   // Section 0 Kind
   ASSERT_CMPIOVEC_BYTES (6);   // Section 0 Body
   ASSERT_CMPIOVEC_UINT8 (7);   // Section 1 Kind
   ASSERT_CMPIOVEC_INT32 (8);   // Section 1 Length
   ASSERT_CMPIOVEC_BYTES (9);   // Section 1 Identifier
   ASSERT_CMPIOVEC_BYTES (10);  // Section 1 Documents
   ASSERT_CMPIOVEC_UINT8 (11);  // Section 2 Kind
   ASSERT_CMPIOVEC_INT32 (12);  // Section 2 Length
   ASSERT_CMPIOVEC_BYTES (13);  // Section 2 Identifier
   ASSERT_CMPIOVEC_BYTES (14);  // Section 2 Documents
   ASSERT_CMPIOVEC_UINT32 (15); // Checksum

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_msg (void)
{
   test_rpc_message_setters_op_msg_kind_0 ();
   test_rpc_message_setters_op_msg_kind_1_single ();
   test_rpc_message_setters_op_msg_kind_1_multiple ();
}

static void
test_rpc_message_setters_op_reply (void)
{
   const uint8_t data[] = {TEST_DATA_OP_REPLY};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 68));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_REPLY));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_reply_set_response_flags (rpc, MONGOC_OP_REPLY_RESPONSE_FLAG_NONE));
   ASSERT_CMPINT32 ( 8, ==, mcd_rpc_op_reply_set_cursor_id (rpc, 1234605616436508552));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_reply_set_starting_from (rpc, 0));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_reply_set_number_returned (rpc, 2));
   ASSERT_CMPINT32 (32, ==, mcd_rpc_op_reply_set_documents (rpc, data + 36, 32u));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 9u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // responseFlags
   ASSERT_CMPIOVEC_INT64 (5); // cursorID
   ASSERT_CMPIOVEC_INT32 (6); // startingFrom
   ASSERT_CMPIOVEC_INT32 (7); // numberReturned
   ASSERT_CMPIOVEC_BYTES (8); // documents

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_update (void)
{
   const uint8_t data[] = {TEST_DATA_OP_UPDATE};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 62));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_UPDATE));
   ASSERT_CMPINT32 ( 8, ==, mcd_rpc_op_update_set_full_collection_name (rpc, (const char *) (data + 20)));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_update_set_flags (rpc, MONGOC_OP_UPDATE_FLAG_NONE));
   ASSERT_CMPINT32 (16, ==, mcd_rpc_op_update_set_selector (rpc, data + 32));
   ASSERT_CMPINT32 (14, ==, mcd_rpc_op_update_set_update (rpc, data + 48));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 9u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // ZERO
   ASSERT_CMPIOVEC_BYTES (5); // fullCollectionName
   ASSERT_CMPIOVEC_INT32 (6); // flags
   ASSERT_CMPIOVEC_BYTES (7); // selector
   ASSERT_CMPIOVEC_BYTES (8); // update

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_insert (void)
{
   const uint8_t data[] = {TEST_DATA_OP_INSERT};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 60));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_INSERT));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_insert_set_flags (rpc, MONGOC_OP_INSERT_FLAG_NONE));
   ASSERT_CMPINT32 ( 8, ==, mcd_rpc_op_insert_set_full_collection_name (rpc, (const char *) (data + 20)));
   ASSERT_CMPINT32 (32, ==, mcd_rpc_op_insert_set_documents (rpc, data + 28, 32u));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 7u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // flags
   ASSERT_CMPIOVEC_BYTES (5); // fullCollectionName
   ASSERT_CMPIOVEC_BYTES (6); // documents

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_query (void)
{
   const uint8_t data[] = {TEST_DATA_OP_QUERY};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 65));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_QUERY));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_query_set_flags (rpc, MONGOC_OP_QUERY_FLAG_NONE));
   ASSERT_CMPINT32 ( 8, ==, mcd_rpc_op_query_set_full_collection_name (rpc, (const char *) (data + 20)));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_query_set_number_to_skip (rpc, 0));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_query_set_number_to_return (rpc, 0));
   ASSERT_CMPINT32 (13, ==, mcd_rpc_op_query_set_query (rpc, data + 36));
   ASSERT_CMPINT32 (16, ==, mcd_rpc_op_query_set_return_fields_selector (rpc, data + 49));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 10u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // flags
   ASSERT_CMPIOVEC_BYTES (5); // fullCollectionName
   ASSERT_CMPIOVEC_INT32 (6); // numberToSkip
   ASSERT_CMPIOVEC_INT32 (7); // numberToReturn
   ASSERT_CMPIOVEC_BYTES (8); // query
   ASSERT_CMPIOVEC_BYTES (9); // returnFieldsSelector

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_get_more (void)
{
   const uint8_t data[] = {TEST_DATA_OP_GET_MORE};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 (4, ==, mcd_rpc_header_set_message_length (rpc, 40));
   ASSERT_CMPINT32 (4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 (4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 (4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_GET_MORE));
   ASSERT_CMPINT32 (8, ==, mcd_rpc_op_get_more_set_full_collection_name (rpc, (const char *) (data + 20)));
   ASSERT_CMPINT32 (4, ==, mcd_rpc_op_get_more_set_number_to_return (rpc, 0));
   ASSERT_CMPINT32 (8, ==, mcd_rpc_op_get_more_set_cursor_id (rpc, 1234605616436508552));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // ZERO
   ASSERT_CMPIOVEC_BYTES (5); // fullCollectionName
   ASSERT_CMPIOVEC_INT32 (6); // numberToReturn
   ASSERT_CMPIOVEC_INT64 (7); // cursorID

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_delete (void)
{
   const uint8_t data[] = {TEST_DATA_OP_DELETE};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 48));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_DELETE));
   ASSERT_CMPINT32 ( 8, ==, mcd_rpc_op_delete_set_full_collection_name (rpc, (const char *) (data + 20)));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_op_delete_set_flags (rpc, MONGOC_OP_DELETE_FLAG_NONE));
   ASSERT_CMPINT32 (16, ==, mcd_rpc_op_delete_set_selector (rpc, data + 32));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 8u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // ZERO
   ASSERT_CMPIOVEC_BYTES (5); // fullCollectionName
   ASSERT_CMPIOVEC_INT32 (6); // flags
   ASSERT_CMPIOVEC_BYTES (7); // selector

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}

static void
test_rpc_message_setters_op_kill_cursors (void)
{
   const uint8_t data[] = {TEST_DATA_OP_KILL_CURSORS};

   mcd_rpc_message *const expected_rpc = mcd_rpc_message_from_data (data, sizeof (data), NULL);
   ASSERT (expected_rpc);

   size_t expected_num_iovecs;
   mongoc_iovec_t *const expected_iovecs = mcd_rpc_message_to_iovecs (expected_rpc, &expected_num_iovecs);
   ASSERT (expected_iovecs);

   const int64_t cursor_ids[] = {1230066625199609624, 2387509390608836392};

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   // clang-format off
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_message_length (rpc, 40));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_request_id (rpc, 16909060));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_response_to (rpc, 84281096));
   ASSERT_CMPINT32 ( 4, ==, mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_KILL_CURSORS));
   ASSERT_CMPINT32 (20, ==, mcd_rpc_op_kill_cursors_set_cursor_ids (rpc, cursor_ids, 2));
   // clang-format on

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   ASSERT (iovecs);

   ASSERT_CMPSIZE_T (num_iovecs, ==, expected_num_iovecs);
   ASSERT_CMPSIZE_T (num_iovecs, ==, 7u);
   ASSERT_CMPIOVEC_INT32 (0); // messageLength
   ASSERT_CMPIOVEC_INT32 (1); // requestID
   ASSERT_CMPIOVEC_INT32 (2); // responseTo
   ASSERT_CMPIOVEC_INT32 (3); // opCode
   ASSERT_CMPIOVEC_INT32 (4); // ZERO
   ASSERT_CMPIOVEC_INT32 (5); // numberOfCursorIds

   // The cursorIDs iovec does not point to original data due to endian
   // conversion requirements. Assert values individually.
   {
      const mongoc_iovec_t *const iovec = iovecs + 6;
      const mongoc_iovec_t *const expected_iovec = expected_iovecs + 6;

      ASSERT_CMPSIZE_T (iovec->iov_len, ==, expected_iovec->iov_len);
      ASSERT (memcmp (iovec->iov_base, expected_iovec->iov_base, expected_iovec->iov_len) == 0);
   }

   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   bson_free (expected_iovecs);
   mcd_rpc_message_destroy (expected_rpc);
}


static void
test_rpc_message_from_data_in_place (void)
{
   const uint8_t data_op_compressed[] = {TEST_DATA_OP_COMPRESSED};
   const uint8_t data_op_msg_kind_0[] = {TEST_DATA_OP_MSG_KIND_0};
   const uint8_t data_op_msg_kind_1_single[] = {TEST_DATA_OP_MSG_KIND_1_SINGLE};
   const uint8_t data_op_msg_kind_1_multiple[] = {TEST_DATA_OP_MSG_KIND_1_MULTIPLE};
   const uint8_t data_op_reply[] = {TEST_DATA_OP_REPLY};
   const uint8_t data_op_update[] = {TEST_DATA_OP_UPDATE};
   const uint8_t data_op_insert[] = {TEST_DATA_OP_INSERT};
   const uint8_t data_op_query[] = {TEST_DATA_OP_QUERY};
   const uint8_t data_op_get_more[] = {TEST_DATA_OP_GET_MORE};
   const uint8_t data_op_delete[] = {TEST_DATA_OP_DELETE};
   const uint8_t data_op_kill_cursors[] = {TEST_DATA_OP_KILL_CURSORS};

   typedef struct test_data_type {
      const uint8_t *data;
      size_t data_len;
   } test_data_type;

   test_data_type test_data[] = {
      {data_op_compressed, sizeof (data_op_compressed)},
      {data_op_msg_kind_0, sizeof (data_op_msg_kind_0)},
      {data_op_msg_kind_1_single, sizeof (data_op_msg_kind_1_single)},
      {data_op_msg_kind_1_multiple, sizeof (data_op_msg_kind_1_multiple)},
      {data_op_reply, sizeof (data_op_reply)},
      {data_op_update, sizeof (data_op_update)},
      {data_op_insert, sizeof (data_op_insert)},
      {data_op_query, sizeof (data_op_query)},
      {data_op_get_more, sizeof (data_op_get_more)},
      {data_op_delete, sizeof (data_op_delete)},
      {data_op_kill_cursors, sizeof (data_op_kill_cursors)},
   };
   const size_t num_test_data = sizeof (test_data) / sizeof (*test_data);

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   ASSERT_CMPINT32 (mcd_rpc_header_get_message_length (rpc), ==, 0);
   ASSERT_CMPINT32 (mcd_rpc_header_get_request_id (rpc), ==, 0);
   ASSERT_CMPINT32 (mcd_rpc_header_get_response_to (rpc), ==, 0);
   ASSERT_CMPINT32 (mcd_rpc_header_get_op_code (rpc), ==, MONGOC_OP_CODE_NONE);

   const void *data_end = NULL;
   bool res = false;

   // Test reuse of RPC message object.
   for (size_t i = 0u; i < num_test_data; ++i) {
      const uint8_t *const data = test_data[i].data;
      const size_t data_len = test_data[i].data_len;

      res = mcd_rpc_message_from_data_in_place (rpc, data, data_len, &data_end);
      ASSERT_RPC_MESSAGE_RESULT (res, data, data_end, data_len);
      mcd_rpc_message_reset (rpc);
   }

   // Again but in reverse order.
   for (size_t i = 0u; i < num_test_data; ++i) {
      const size_t idx = num_test_data - 1u - i;
      const uint8_t *const data = test_data[idx].data;
      const size_t data_len = test_data[idx].data_len;

      res = mcd_rpc_message_from_data_in_place (rpc, data, data_len, &data_end);
      ASSERT_RPC_MESSAGE_RESULT (res, data, data_end, data_len);
      mcd_rpc_message_reset (rpc);
   }

   // Also test post-conversion to iovecs and little endian.
   for (size_t i = 0u; i < num_test_data; ++i) {
      const uint8_t *const data = test_data[i].data;
      const size_t data_len = test_data[i].data_len;

      res = mcd_rpc_message_from_data_in_place (rpc, data, data_len, &data_end);
      ASSERT_RPC_MESSAGE_RESULT (res, data, data_end, data_len);

      size_t num_iovecs;
      mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
      ASSERT (iovecs && num_iovecs > 0u);
      bson_free (iovecs);

      mcd_rpc_message_reset (rpc);
   }

   // Again but in reverse order.
   for (size_t i = 0u; i < num_test_data; ++i) {
      const size_t idx = num_test_data - 1u - i;
      const uint8_t *const data = test_data[idx].data;
      const size_t data_len = test_data[idx].data_len;

      res = mcd_rpc_message_from_data_in_place (rpc, data, data_len, &data_end);
      ASSERT_RPC_MESSAGE_RESULT (res, data, data_end, data_len);

      size_t num_iovecs;
      mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
      ASSERT (iovecs && num_iovecs > 0u);
      bson_free (iovecs);

      mcd_rpc_message_reset (rpc);
   }

   mcd_rpc_message_destroy (rpc);
}


void
test_mcd_rpc_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/rpc_message/from_data/op_compressed/valid", test_rpc_message_from_data_op_compressed_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_msg/valid", test_rpc_message_from_data_op_msg_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_reply/valid", test_rpc_message_from_data_op_reply_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_update/valid", test_rpc_message_from_data_op_update_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_insert/valid", test_rpc_message_from_data_op_insert_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_query/valid", test_rpc_message_from_data_op_query_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_get_more/valid", test_rpc_message_from_data_op_get_more_valid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_delete/valid", test_rpc_message_from_data_op_delete_valid);
   TestSuite_Add (
      suite, "/rpc_message/from_data/op_kill_cursors/valid", test_rpc_message_from_data_op_kill_cursors_valid);

   TestSuite_Add (
      suite, "/rpc_message/from_data/op_compressed/invalid", test_rpc_message_from_data_op_compressed_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_msg/invalid", test_rpc_message_from_data_op_msg_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_reply/invalid", test_rpc_message_from_data_op_reply_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_update/invalid", test_rpc_message_from_data_op_update_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_insert/invalid", test_rpc_message_from_data_op_insert_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_query/invalid", test_rpc_message_from_data_op_query_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_get_more/invalid", test_rpc_message_from_data_op_get_more_invalid);
   TestSuite_Add (suite, "/rpc_message/from_data/op_delete/invalid", test_rpc_message_from_data_op_delete_invalid);
   TestSuite_Add (
      suite, "/rpc_message/from_data/op_kill_cursors/invalid", test_rpc_message_from_data_op_kill_cursors_invalid);

   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_compressed", test_rpc_message_to_iovecs_op_compressed);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_msg", test_rpc_message_to_iovecs_op_msg);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_reply", test_rpc_message_to_iovecs_op_reply);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_update", test_rpc_message_to_iovecs_op_update);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_insert", test_rpc_message_to_iovecs_op_insert);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_query", test_rpc_message_to_iovecs_op_query);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_get_more", test_rpc_message_to_iovecs_op_get_more);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_delete", test_rpc_message_to_iovecs_op_delete);
   TestSuite_Add (suite, "/rpc_message/to_iovecs/op_kill_cursors", test_rpc_message_to_iovecs_op_kill_cursors);

   TestSuite_Add (suite, "/rpc_message/setters/op_compressed", test_rpc_message_setters_op_compressed);
   TestSuite_Add (suite, "/rpc_message/setters/op_msg", test_rpc_message_setters_op_msg);
   TestSuite_Add (suite, "/rpc_message/setters/op_reply", test_rpc_message_setters_op_reply);
   TestSuite_Add (suite, "/rpc_message/setters/op_update", test_rpc_message_setters_op_update);
   TestSuite_Add (suite, "/rpc_message/setters/op_insert", test_rpc_message_setters_op_insert);
   TestSuite_Add (suite, "/rpc_message/setters/op_query", test_rpc_message_setters_op_query);
   TestSuite_Add (suite, "/rpc_message/setters/op_get_more", test_rpc_message_setters_op_get_more);
   TestSuite_Add (suite, "/rpc_message/setters/op_delete", test_rpc_message_setters_op_delete);
   TestSuite_Add (suite, "/rpc_message/setters/op_kill_cursors", test_rpc_message_setters_op_kill_cursors);


   TestSuite_Add (suite, "/rpc_message/from_data/in_place", test_rpc_message_from_data_in_place);
}
