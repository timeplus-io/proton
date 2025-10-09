#ifdef __MINGW32__
#  define __USE_MINGW_ANSI_STDIO 1
#endif
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "getopt.h"
#include "../ulid.h"

static void
usage(FILE *f)
{
    fprintf(f, "usage: ulidgen -G [-prs] [-n N]\n");
    fprintf(f, "       ulidgen -C [-iq] <ULIDs...>\n");
    fprintf(f, "       ulidgen -T [-i] <ULIDs...>\n");
    fprintf(f, "       ulidgen -h\n");
    fprintf(f, "  -G      Generate ULIDs\n");
    fprintf(f, "  -C      Check/validate ULIDs\n");
    fprintf(f, "  -T      Print timestamp from ULIDs\n");
    fprintf(f, "  -h      Display this help message\n");
    fprintf(f, "  -i      (-C|-T) Read ULIDs on standard input\n");
    fprintf(f, "  -n N    (-G) Number of ULIDs to generate [1]\n");
    fprintf(f, "  -p      (-G) Only use 79 random bits to avoid overflow\n");
    fprintf(f, "  -q      (-C) Don't print invalid ULIDs\n");
    fprintf(f, "  -r      (-G) Non-monotonic ULIDs within timestamp\n");
    fprintf(f, "  -s      (-G) Require secure initialization\n");
}

static int
validate(unsigned char buf[16], char *s)
{
    if (ulid_decode(buf, s))
        return 1;
    if (s[26] != '\r' && s[26] != '\n' && s[26] != 0)
        return 1;
    return 0;
}

static void
ts_print(const unsigned char buf[16])
{
    unsigned long long ts =
        (unsigned long long)buf[0] << 40 |
        (unsigned long long)buf[1] << 32 |
        (unsigned long long)buf[2] << 24 |
        (unsigned long long)buf[3] << 16 |
        (unsigned long long)buf[4] <<  8 |
        (unsigned long long)buf[5] <<  0;
    printf("%llu.%03llu\n", ts / 1000, ts % 1000);
}

int
main(int argc, char *argv[])
{
    enum {
        MODE_NONE,
        MODE_GENERATE,
        MODE_CHECK,
        MODE_TIMESTAMP
    } mode = MODE_NONE;
    enum {
        SOURCE_ARGV,
        SOURCE_STDIN
    } source = SOURCE_ARGV;
    int flags = 0;
    int quiet = 0;
    long count = 1;

    int option;
    while ((option = getopt(argc, argv, "CGThipn:qrs")) != -1) {
        switch (option) {
            case 'C': {
                mode = MODE_CHECK;
            } break;
            case 'G': {
                mode = MODE_GENERATE;
            } break;
            case 'T': {
                mode = MODE_TIMESTAMP;
            } break;
            case 'h': {
                usage(stdout);
                exit(EXIT_SUCCESS);
            } break;
            case 'i': {
                source = SOURCE_STDIN;
            } break;
            case 'n': {
                char *endptr;
                errno = 0;
                count = strtol(optarg, &endptr, 10);
                if (errno || *endptr || count < 0) {
                    fprintf(stderr, "ulidgen: invalid count -- %s\n", optarg);
                    exit(EXIT_FAILURE);
                }
            } break;
            case 'p': {
                flags |= ULID_PARANOID;
            } break;
            case 'q': {
                quiet = 1;
            } break;
            case 'r': {
                flags |= ULID_RELAXED;
            } break;
            case 's': {
                flags |= ULID_SECURE;
            } break;
            default: {
                usage(stderr);
                exit(EXIT_FAILURE);
            } break;
        }
    }

    switch (mode) {
        case MODE_NONE: {
            usage(stderr);
            exit(EXIT_FAILURE);
        } break;

        case MODE_CHECK: {
            int result = EXIT_SUCCESS;
            switch (source) {
                case SOURCE_ARGV: {
                    unsigned char buf[16];
                    for (int i = optind; argv[i]; i++) {
                        if (validate(buf, argv[i])) {
                            if (!quiet) puts(argv[i]);
                            result = EXIT_FAILURE;
                        }
                    }
                } break;
                case SOURCE_STDIN: {
                    char line[29] = {0};
                    unsigned char buf[16];
                    while (fgets(line, sizeof(line), stdin)) {
                        if (validate(buf, line)) {
                            if (!quiet) puts(line);
                            result = EXIT_FAILURE;
                        }
                    }
                } break;
            }
            exit(result);
        } break;

        case MODE_GENERATE: {
            struct ulid_generator ulidgen[1];
            int r = ulid_generator_init(ulidgen, flags);
            if (r != 0 && (flags & ULID_SECURE)) {
                fprintf(stderr, "ulidgen: failed to get secure entropy\n");
                exit(EXIT_FAILURE);
            }
            while (count--) {
                char ulid[27];
                ulid_generate(ulidgen, ulid);
                puts(ulid);
            }
        } break;

        case MODE_TIMESTAMP: {
            switch (source) {
                case SOURCE_ARGV: {
                    unsigned char buf[16];
                    for (int i = optind; argv[i]; i++) {
                        if (validate(buf, argv[i]))
                            exit(EXIT_FAILURE);
                        ts_print(buf);
                    }
                } break;
                case SOURCE_STDIN: {
                    char line[29] = {0};
                    unsigned char buf[16];
                    while (fgets(line, sizeof(line), stdin)) {
                        if (validate(buf, line))
                            exit(EXIT_FAILURE);
                        ts_print(buf);
                    }
                } break;
            }
        } break;
    }

    if (fflush(stdout) || ferror(stdout)) {
        fprintf(stderr, "ulidgen: output error\n");
        exit(EXIT_FAILURE);
    }
}
