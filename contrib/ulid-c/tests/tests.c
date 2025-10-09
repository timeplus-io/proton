#include <stdio.h>
#include <string.h>
#include "../ulid.h"

#define TEST(x, s) \
    do { \
        if (x) { \
            printf("\033[32;1mPASS\033[0m %s\n", s); \
            count_pass++; \
        } else { \
            printf("\033[31;1mFAIL\033[0m %s\n", s); \
            count_fail++; \
        } \
    } while (0)

int
main(void)
{
    int count_pass = 0;
    int count_fail = 0;

    {
        const char *expect = "00000000000000000000000000";
        unsigned char bin[16] = {0};
        char ulid[27];
        ulid_encode(ulid, bin);
        TEST(!strcmp(ulid, expect), "minimum encode");
    }

    {
        const char *expect = "7ZZZZZZZZZZZZZZZZZZZZZZZZZ";
        unsigned char bin[16];
        memset(bin, 0xff, sizeof(bin));
        char ulid[27];
        ulid_encode(ulid, bin);
        TEST(!strcmp(ulid, expect), "maximum encode");
    }

    {
        unsigned char bin[16];
        char ulid[] = "80000000000000000000000000";
        TEST(ulid_decode(bin, ulid), "reject too large");
    }

    {
        const unsigned char expect[] = {
            0x21, 0x08, 0x42, 0x00, 0x01, 0x4b, 0x63, 0x5c,
            0xf8, 0x44, 0x32, 0x98, 0x69, 0x50, 0x5a, 0xf8
        };
        unsigned char bin[16];
        char ulid[] = "1IiLl0Ooabcdefghijklmnopqr";
        TEST(!ulid_decode(bin, ulid), "alternate decode (valid)");
        TEST(!memcmp(bin, expect, sizeof(bin)), "alternate decode (check)");
    }

    {
        const unsigned char expect[] = {
            0x01, 0x23, 0x45, 0x56, 0x67, 0x89, 0xab, 0xcd,
            0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32
        };
        char ulid[27];
        unsigned char bin[16];
        ulid_encode(ulid, expect);
        ulid_decode(bin, ulid);
        TEST(!memcmp(bin, expect, sizeof(bin)), "there and back again");
    }

    {
        /* Generate a million ULIDs and make sure they're all ordered. */
        struct ulid_generator g[1];
        ulid_generator_init(g, 0);
        int pass = 1;
        char ulid[2][27];
        ulid_generate(g, ulid[1]);
        for (long i = 0; i < 1L << 20; i++) {
            char *a = ulid[  i % 2 ];
            char *b = ulid[!(i % 2)];
            ulid_generate(g, a);
            if (strcmp(a, b) <= 0)
                pass = 0;

            /* Also validate it while we're here. */
            unsigned char bin[16];
            if (ulid_decode(bin, a))
                pass = 0;
        }
        TEST(pass, "monotonicity");
    }

    printf("%d fail, %d pass\n", count_fail, count_pass);
    return count_fail != 0;
}
