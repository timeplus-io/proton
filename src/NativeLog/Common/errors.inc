#ifndef ERROR_CODE
#error ERROR_CODE() macro not defined
#define ERROR_CODE(...)
#endif

ERROR_CODE(OK, 0, "success")
ERROR_CODE(NOT_FOUND, 1, "NOT_FOUND")
ERROR_CODE(OFFSET_OUT_OF_RANGE, 2, "offset out of range")

#undef ERROR_CODE
