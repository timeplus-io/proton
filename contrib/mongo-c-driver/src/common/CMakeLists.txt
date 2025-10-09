
set (MONGOC_ENABLE_DEBUG_ASSERTIONS 0)

if (ENABLE_DEBUG_ASSERTIONS)
    set (MONGOC_ENABLE_DEBUG_ASSERTIONS 1)
endif ()

include (MongoC-Warnings)

configure_file (
        "${PROJECT_SOURCE_DIR}/src/common/common-config.h.in"
        "${PROJECT_BINARY_DIR}/src/common/common-config.h"
)
