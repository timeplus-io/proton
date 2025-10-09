include (CheckSymbolExists)

# Allow old "SYSTEM" option but prefer ON/AUTO/OFF.
if (NOT ENABLE_SNAPPY MATCHES "ON|SYSTEM|AUTO|OFF")
   message (FATAL_ERROR "ENABLE_SNAPPY option must be ON, AUTO, or OFF")
endif ()

if (NOT ENABLE_SNAPPY STREQUAL OFF)
   message (STATUS "Searching for compression library header snappy-c.h")
   find_path (
      SNAPPY_INCLUDE_DIRS NAMES snappy-c.h
      PATHS /include /usr/include /usr/local/include /usr/share/include /opt/include c:/snappy/include
      DOC "Searching for snappy-c.h")

   if (NOT SNAPPY_INCLUDE_DIRS)
      if (ENABLE_SNAPPY MATCHES "ON|SYSTEM")
         message (FATAL_ERROR "  Not found (specify -DCMAKE_INCLUDE_PATH=/path/to/snappy/include for Snappy compression)")
      else ()
         message (STATUS "  Not found (specify -DCMAKE_INCLUDE_PATH=/path/to/snappy/include for Snappy compression)")
      endif ()
   else ()
      message (STATUS "  Found in ${SNAPPY_INCLUDE_DIRS}")
      message (STATUS "Searching for libsnappy")
      find_library (
         SNAPPY_LIBRARIES NAMES snappy
         PATHS /usr/lib /lib /usr/local/lib /usr/share/lib /opt/lib /opt/share/lib /var/lib c:/snappy/lib
         DOC "Searching for libsnappy")

      if (SNAPPY_LIBRARIES)
         message (STATUS "  Found ${SNAPPY_LIBRARIES}")
      else ()
         if (ENABLE_SNAPPY MATCHES "ON|SYSTEM")
            message (FATAL_ERROR "  Not found (specify -DCMAKE_LIBRARY_PATH=/path/to/snappy/lib for Snappy compression)")
         else ()
            message (STATUS "  Not found (specify -DCMAKE_LIBRARY_PATH=/path/to/snappy/lib for Snappy compression)")
         endif ()
      endif ()
   endif ()

   if (SNAPPY_INCLUDE_DIRS AND SNAPPY_LIBRARIES)
      set (MONGOC_ENABLE_COMPRESSION_SNAPPY 1)
      set (MONGOC_ENABLE_COMPRESSION 1)
   endif ()
endif ()

if (NOT SNAPPY_INCLUDE_DIRS OR NOT SNAPPY_LIBRARIES)
   set (SNAPPY_INCLUDE_DIRS "")
   set (SNAPPY_LIBRARIES "")
   set (MONGOC_ENABLE_COMPRESSION_SNAPPY 0)
endif ()
