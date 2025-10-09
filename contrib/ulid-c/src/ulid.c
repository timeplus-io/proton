/* ULID generation and parsing library (implementation)
 *
 * This is free and unencumbered software released into the public domain.
 */
#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  pragma comment(lib, "advapi32.lib")
#elif __linux__
#  define _GNU_SOURCE
#  include <unistd.h>
#  include <sys/time.h>
#  include <sys/syscall.h>
#else
#  define _POSIX_C_SOURCE 200112L
#  include <sys/time.h>
#endif
#include <time.h>
#include <stdio.h>
#include <string.h>
#include "ulid.h"

/* Returns unix epoch microseconds.
 */
static unsigned long long
platform_utime(int coarse)
{
#ifdef _WIN32
    FILETIME ft;
    (void)coarse;
    GetSystemTimeAsFileTime(&ft);
    return ((unsigned long long)ft.dwHighDateTime << 32 |
            (unsigned long long)ft.dwLowDateTime  <<  0)
        / 10 - 11644473600000000ULL;
#elif __linux__
    /* CLOCK_REALTIME_COARSE has a resolution of 1ms, which is
     * sufficient for this purpose. It's also _much_ faster.
     */
    struct timespec tv[1];
    clock_gettime(coarse ? CLOCK_REALTIME_COARSE : CLOCK_REALTIME, tv);
    return tv->tv_sec * 1000000ULL + tv->tv_nsec / 1000ULL;
#else
    struct timespec tv[1];
    (void)coarse;
    clock_gettime(CLOCK_REALTIME, tv);
    return tv->tv_sec * 1000000ULL + tv->tv_nsec / 1000ULL;
#endif
}

/* Gather entropy from the operating system.
 * Returns 0 on success.
 */
static int
platform_entropy(void *buf, int len)
{
#if _WIN32
    BOOLEAN NTAPI SystemFunction036(PVOID, ULONG);
    return !SystemFunction036(buf, len);
#elif __linux__
    return syscall(SYS_getrandom, buf, len, 0) != len;
#else
    int r = 0;
    FILE *f = fopen("/dev/urandom", "rb");
    if (f) {
        r = fread(buf, len, 1, f);
        fclose(f);
    }
    return !r;
#endif
}

int
ulid_generator_init(struct ulid_generator *g, int flags)
{
    g->last_ts = 0;
    g->flags = flags;
    g->i = g->j = 0;
    for (int i = 0; i < 256; i++)
        g->s[i] = i;

    /* RC4 is used to fill the random segment of ULIDs. It's tiny,
     * simple, perfectly sufficient for the task (assuming it's seeded
     * properly), and doesn't require fixed-width integers. It's not the
     * fastest option, but it's plenty fast for the task.
     *
     * Besides, when we're in a serious hurry in normal operation (not
     * in "relaxed" mode), we're incrementing the random field much more
     * often than generating fresh random bytes.
     */

    int initstyle = 1;
    unsigned char key[256] = {0};
    if (!platform_entropy(key, 256)) {
        /* Mix entropy into the RC4 state. */
        for (int i = 0, j = 0; i < 256; i++) {
            j = (j + g->s[i] + key[i]) & 0xff;
            int tmp = g->s[i];
            g->s[i] = g->s[j];
            g->s[j] = tmp;
        }
        initstyle = 0;
    } else if (!(flags & ULID_SECURE)) {
        /* Failed to read entropy from OS, so generate some. */
        unsigned long n = 0;
        unsigned long long now;
        unsigned long long start = platform_utime(0);
        do {
            struct {
                clock_t clk;
                unsigned long long ts;
                long n;
                void *stackgap;
            } noise;
            noise.ts = now = platform_utime(0);
            noise.clk = clock();
            noise.stackgap = &noise;
            noise.n = n;
            unsigned char *k = (unsigned char *)&noise;
            for (int i = 0, j = 0; i < 256; i++) {
                j = (j + g->s[i] + k[i % sizeof(noise)]) & 0xff;
                int tmp = g->s[i];
                g->s[i] = g->s[j];
                g->s[j] = tmp;
            }
        } while (n++ < 1UL << 16 || now - start < 500000ULL);
    }
    return initstyle;
}

void
ulid_encode(char str[26], const unsigned char ulid[16])
{
    static const char set[256] = {
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x4a, 0x4b, 0x4d, 0x4e, 0x50, 0x51,
        0x52, 0x53, 0x54, 0x56, 0x57, 0x58, 0x59, 0x5a
    };
    str[ 0] = set[ ulid[ 0] >> 5];
    str[ 1] = set[ ulid[ 0] >> 0];
    str[ 2] = set[ ulid[ 1] >> 3];
    str[ 3] = set[(ulid[ 1] << 2 | ulid[ 2] >> 6) & 0x1f];
    str[ 4] = set[ ulid[ 2] >> 1];
    str[ 5] = set[(ulid[ 2] << 4 | ulid[ 3] >> 4) & 0x1f];
    str[ 6] = set[(ulid[ 3] << 1 | ulid[ 4] >> 7) & 0x1f];
    str[ 7] = set[ ulid[ 4] >> 2];
    str[ 8] = set[(ulid[ 4] << 3 | ulid[ 5] >> 5) & 0x1f];
    str[ 9] = set[ ulid[ 5] >> 0];
    str[10] = set[ ulid[ 6] >> 3];
    str[11] = set[(ulid[ 6] << 2 | ulid[ 7] >> 6) & 0x1f];
    str[12] = set[ ulid[ 7] >> 1];
    str[13] = set[(ulid[ 7] << 4 | ulid[ 8] >> 4) & 0x1f];
    str[14] = set[(ulid[ 8] << 1 | ulid[ 9] >> 7) & 0x1f];
    str[15] = set[ ulid[ 9] >> 2];
    str[16] = set[(ulid[ 9] << 3 | ulid[10] >> 5) & 0x1f];
    str[17] = set[ ulid[10] >> 0];
    str[18] = set[ ulid[11] >> 3];
    str[19] = set[(ulid[11] << 2 | ulid[12] >> 6) & 0x1f];
    str[20] = set[ ulid[12] >> 1];
    str[21] = set[(ulid[12] << 4 | ulid[13] >> 4) & 0x1f];
    str[22] = set[(ulid[13] << 1 | ulid[14] >> 7) & 0x1f];
    str[23] = set[ ulid[14] >> 2];
    str[24] = set[(ulid[14] << 3 | ulid[15] >> 5) & 0x1f];
    str[25] = set[ ulid[15] >> 0];
}

int
ulid_decode(unsigned char ulid[16], const char *s)
{
    static const signed char v[] = {
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09,   -1,   -1,   -1,   -1,   -1,   -1,
          -1, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        0x11, 0x01, 0x12, 0x13, 0x01, 0x14, 0x15, 0x00,
        0x16, 0x17, 0x18, 0x19, 0x1a,   -1, 0x1b, 0x1c,
        0x1d, 0x1e, 0x1f,   -1,   -1,   -1,   -1,   -1,
          -1, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        0x11, 0x01, 0x12, 0x13, 0x01, 0x14, 0x15, 0x00,
        0x16, 0x17, 0x18, 0x19, 0x1a,   -1, 0x1b, 0x1c,
        0x1d, 0x1e, 0x1f,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
          -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1
    };
    if (v[(int)s[0]] > 7)
        return 1;
    for (int i = 0; i < 26; i++)
        if (v[(int)s[i]] == -1)
            return 2;
    ulid[ 0] = v[(int)s[ 0]] << 5 | v[(int)s[ 1]] >> 0;
    ulid[ 1] = v[(int)s[ 2]] << 3 | v[(int)s[ 3]] >> 2;
    ulid[ 2] = v[(int)s[ 3]] << 6 | v[(int)s[ 4]] << 1 | v[(int)s[ 5]] >> 4;
    ulid[ 3] = v[(int)s[ 5]] << 4 | v[(int)s[ 6]] >> 1;
    ulid[ 4] = v[(int)s[ 6]] << 7 | v[(int)s[ 7]] << 2 | v[(int)s[ 8]] >> 3;
    ulid[ 5] = v[(int)s[ 8]] << 5 | v[(int)s[ 9]] >> 0;
    ulid[ 6] = v[(int)s[10]] << 3 | v[(int)s[11]] >> 2;
    ulid[ 7] = v[(int)s[11]] << 6 | v[(int)s[12]] << 1 | v[(int)s[13]] >> 4;
    ulid[ 8] = v[(int)s[13]] << 4 | v[(int)s[14]] >> 1;
    ulid[ 9] = v[(int)s[14]] << 7 | v[(int)s[15]] << 2 | v[(int)s[16]] >> 3;
    ulid[10] = v[(int)s[16]] << 5 | v[(int)s[17]] >> 0;
    ulid[11] = v[(int)s[18]] << 3 | v[(int)s[19]] >> 2;
    ulid[12] = v[(int)s[19]] << 6 | v[(int)s[20]] << 1 | v[(int)s[21]] >> 4;
    ulid[13] = v[(int)s[21]] << 4 | v[(int)s[22]] >> 1;
    ulid[14] = v[(int)s[22]] << 7 | v[(int)s[23]] << 2 | v[(int)s[24]] >> 3;
    ulid[15] = v[(int)s[24]] << 5 | v[(int)s[25]] >> 0;
    return 0;
}

void
ulid_generate(struct ulid_generator *g, char str[26])
{
    unsigned long long ts = platform_utime(1) / 1000;

    if (!(g->flags & ULID_RELAXED) && g->last_ts == ts) {
        /* Chance of 80-bit overflow is so small that it's not considered. */
        for (int i = 15; i > 5; i--)
            if (++g->last[i])
                break;
        ulid_encode(str, g->last);
        return;
    }

    /* Fill out timestamp */
    g->last_ts = ts;
    g->last[0] = ts >> 40;
    g->last[1] = ts >> 32;
    g->last[2] = ts >> 24;
    g->last[3] = ts >> 16;
    g->last[4] = ts >>  8;
    g->last[5] = ts >>  0;

    /* Fill out random section */
    for (int k = 0; k < 10; k++) {
        g->i = (g->i + 1) & 0xff;
        g->j = (g->j + g->s[g->i]) & 0xff;
        int tmp = g->s[g->i];
        g->s[g->i] = g->s[g->j];
        g->s[g->j] = tmp;
        g->last[6 + k] = g->s[(g->s[g->i] + g->s[g->j]) & 0xff];
    }
    if (g->flags & ULID_PARANOID)
        g->last[6] &= 0x7f;

    ulid_encode(str, g->last);
}

#ifdef __cplusplus
}
#endif
