include (CheckSymbolExists)

check_symbol_exists (sched_getcpu sched.h HAVE_SCHED_GETCPU)
if (HAVE_SCHED_GETCPU)
   set (MONGOC_HAVE_SCHED_GETCPU 1)
else ()
   set (MONGOC_HAVE_SCHED_GETCPU 0)
endif ()
