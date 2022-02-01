/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_COMPAT_H
#define LIBURING_COMPAT_H

typedef int64_t __kernel_time64_t;

#include <linux/time_types.h>

#include <inttypes.h>

struct open_how {
	uint64_t	flags;
	uint64_t	mode;
	uint64_t	resolve;
};

#endif
