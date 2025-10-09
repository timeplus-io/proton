# sub-directory 'libbson' was already included at the top-level
# sub-directory 'libmongoc' was already included at the top-level
# sub-directory 'kms-message' was already included at the top-level

# Copy zconf.h.in to zconf.h, used by zlib
configure_file (
   "${SOURCE_DIR}/src/zlib-1.3.1/zconf.h.in"
   "${CMAKE_BINARY_DIR}/src/zlib-1.3.1/zconf.h"
   COPYONLY
)

if (CMAKE_CXX_COMPILER)
   # Add a C++ source file that will #include the main C headers. This "library"
   # does nothing other than validate that the C headers are valid C++ headers.
   add_library (mongoc-cxx-check STATIC cpp-check.cpp)
   if (TARGET mongoc_static)
      target_link_libraries (mongoc-cxx-check PRIVATE mongoc_static)
   else ()
      target_link_libraries (mongoc-cxx-check PRIVATE mongoc_shared)
   endif ()
endif ()
