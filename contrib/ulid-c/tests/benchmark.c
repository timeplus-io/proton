#include "../ulid.c"  // poor man's LTO
#include <setjmp.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#define SECS_PER_TEST   1
#define NUM_TESTS       3

static jmp_buf finish;

static void
alarm_handler(int signum)
{
    (void)signum;
    siglongjmp(finish, 1);
}

/* Make intermediate benchmark results visible outside of the benchmark.
 * This helps prevent the optimizer from removing things we don't want
 * it to.
 */
char benchmark_ulid[27];
uint64_t benchmark_sum = 0;

static long
benchmark_generate(struct ulid_generator *g)
{
    /* Technically best doesn't need to be volatile since it isn't
     * modified between setjmp() and longjmp(), but GCC seems to be
     * confused about this.
     */
    volatile long best = 0;
    for (int i = 0; i < NUM_TESTS; i++) {
        volatile unsigned long count = 0;
        unsigned long long start = platform_utime(0);
        if (!sigsetjmp(finish, 1)) {
            signal(SIGALRM, alarm_handler);
            alarm(SECS_PER_TEST);
            for (;;) {
                ulid_generate(g, benchmark_ulid);
                count++;
            }
        }
        double dt = (platform_utime(0) - start) / 1000000.0;
        long result = count / dt;
        if (result > best)
            best = result;
    }
    return best;
}

static long
benchmark_decode(void)
{
    struct ulid_generator g[1];
    ulid_generator_init(g, ULID_RELAXED);

    long best = 0;
    for (int i = 0; i < NUM_TESTS; i++) {

        /* Fill out a table of fresh, random ULIDs */
        static char ulids[1L << 20][27];
        for (size_t i = 0; i < sizeof(ulids) / sizeof(*ulids); i++) {
            ulid_generate(g, ulids[i]);
            /* Mix up the ULID with alternate Base32 input characters. */
            for (int j = 0; j < 26; j++) {
                if (ulids[i][j] == '0') {
                    ulids[i][j] = "0Oo"[rand() % 3];
                } else if (ulids[i][j] == '2') {
                    ulids[i][j] = "1IiLl"[rand() % 5];
                } else if (ulids[i][j] >= 'A' && ulids[i][j] <= 'Z') {
                    ulids[i][j] -= (rand() % 2) * ('A' - 'a');
                }
            }
        }

        /* Parse the entire table and measure the time. */
        unsigned long long start = platform_utime(0);
        for (size_t i = 0; i < sizeof(ulids) / sizeof(*ulids); i++) {
            uint64_t bin[2];
            benchmark_sum += ulid_decode((unsigned char *)bin, ulids[i]);
            benchmark_sum += bin[0] + bin[1];
        }
        double dt = (platform_utime(0) - start) / 1000000.0;
        long result = sizeof(ulids) / sizeof(*ulids) / dt;
        if (result > best)
            best = result;
    }

    return best;
}

int
main(void)
{
    /* Used to "consume" critical parts of the final generator state and
     * output, requiring these values to actually be computed. This
     * prevents ULID generation from being optimized out.
     */
    volatile unsigned sink = 0;

    {
        printf("ulid_decode()              %8ld kULID / s\n",
                benchmark_decode() / 1000);
        sink += benchmark_sum;
        sink += benchmark_sum >> 32;
    }

    {
        struct ulid_generator g[1];
        ulid_generator_init(g, 0);
        printf("ulid_generate() [standard] %8ld kULID / s\n",
                benchmark_generate(g) / 1000);
        sink += g->j;
        for (int i = 0; i < 27; i++)
            sink += benchmark_ulid[i];
    }

    {
        struct ulid_generator g[1];
        ulid_generator_init(g, ULID_RELAXED);
        printf("ulid_generate() [relaxed]  %8ld kULID / s\n",
                benchmark_generate(g) / 1000);
        sink += g->j;
        for (int i = 0; i < 27; i++)
            sink += benchmark_ulid[i];
    }
}
