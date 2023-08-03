#pragma once

#if (defined(__GNUC__) && __GNUC__ >= 3) || defined(__clang__)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)
#else
#define unlikely(x) (x)
#define likely(x) (x)
#endif

#define ART_MAX_PREFIX_LEN (8)
