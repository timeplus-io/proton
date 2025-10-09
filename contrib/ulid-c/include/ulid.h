/* ULID generation and parsing library (API)
 *
 * This is free and unencumbered software released into the public domain.
 */
#ifndef ULID_H
#define ULID_H

#ifdef __cplusplus
extern "C" {
#endif

/* Generator configuration flags */
#define ULID_RELAXED   (1 << 0)
#define ULID_PARANOID  (1 << 1)
#define ULID_SECURE    (1 << 2)

struct ulid_generator {
    unsigned char last[16];
    unsigned long long last_ts;
    int flags;
    unsigned char i, j;
    unsigned char s[256];
};

/* Initialize a new ULID generator instance.
 *
 * The ULID_RELAXED flag allows ULIDs generated within the same
 * millisecond to be non-monotonic, e.g. the random section is generated
 * fresh each time.
 *
 * The ULID_PARANOID flag causes the generator to clear the highest bit
 * of the random field, which guarantees that overflow cannot occur.
 * Normally the chance of overflow is non-zero, but negligible. This
 * makes it zero. It doesn't make sense to use this flag in conjunction
 * with ULID_RELAX.
 *
 * The ULID_SECURE flag doesn't fall back on userspace initialization if
 * system entropy could not be gathered. You _must_ check the return
 * value if you use this flag, since it now indicates a hard error.
 *
 * Returns 0 if the generator was successfully initialized from secure
 * system entropy. Returns 1 if this failed and instead derived entropy
 * in userspace (or is uninitialized in the case of ULID_SECURE).
 */
int  ulid_generator_init(struct ulid_generator *, int flags);

/* Generate a new ULID.
 */
void ulid_generate(struct ulid_generator *, char [26]);

/* Encode a 128-bit binary ULID to its text format.
 */
void ulid_encode(char [26], const unsigned char [16]);

/* Decode a text ULID to a 128-bit binary ULID.
 * Returns non-zero if input was invalid.
 */
int  ulid_decode(unsigned char [16], const char *);

#ifdef __cplusplus
}
#endif

#endif
