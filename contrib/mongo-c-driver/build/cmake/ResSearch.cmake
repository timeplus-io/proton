include(CheckSymbolExists)
include(CMakePushCheckState)

cmake_push_check_state()

# The name of the library that performs name resolution, suitable for giving to the "-l" link flag
set(RESOLVE_LIB_NAME)
# If TRUE, then the C runtime provides the name resolution that we need
set(resolve_is_libc FALSE)

if(WIN32)
    set(RESOLVE_LIB_NAME Dnsapi)
    set(_MONGOC_HAVE_DNSAPI 1)
else()
    # Try to find the search functions for various configurations.
    # Headers required by minimum on the strictest system: (Tested on FreeBSD 13)
    set(resolve_headers netinet/in.h sys/types.h arpa/nameser.h resolv.h)
    set(CMAKE_REQUIRED_LIBRARIES resolv)
    check_symbol_exists(res_nsearch "${resolve_headers}" _MONGOC_HAVE_RES_NSEARCH_RESOLV)
    check_symbol_exists(res_search "${resolve_headers}" _MONGOC_HAVE_RES_SEARCH_RESOLV)
    check_symbol_exists(res_ndestroy "${resolve_headers}" _MONGOC_HAVE_RES_NDESTROY_RESOLV)
    check_symbol_exists(res_nclose "${resolve_headers}" _MONGOC_HAVE_RES_NCLOSE_RESOLV)
    if(
        (_MONGOC_HAVE_RES_NSEARCH_RESOLV
            AND (_MONGOC_HAVE_RES_NDESTROY_RESOLV OR _MONGOC_HAVE_RES_NCLOSE_RESOLV))
        OR _MONGOC_HAVE_RES_SEARCH_RESOLV
    )
        set(RESOLVE_LIB_NAME resolv)
    else()
        # Can we use name resolution with just libc?
        unset(CMAKE_REQUIRED_LIBRARIES)
        check_symbol_exists(res_nsearch "${resolve_headers}" _MONGOC_HAVE_RES_NSEARCH_NOLINK)
        check_symbol_exists(res_search "${resolve_headers}" _MONGOC_HAVE_RES_SEARCH_NOLINK)
        check_symbol_exists(res_ndestroy "${resolve_headers}" _MONGOC_HAVE_RES_NDESTROY_NOLINK)
        check_symbol_exists(res_nclose "${resolve_headers}" _MONGOC_HAVE_RES_NCLOSE_NOLINK)
        if(
            (_MONGOC_HAVE_RES_NSEARCH_NOLINK
                AND (_MONGOC_HAVE_RES_NDESTROY_NOLINK OR _MONGOC_HAVE_RES_NCLOSE_NOLINK))
            OR _MONGOC_HAVE_RES_SEARCH_NOLINK
        )
            set(resolve_is_libc TRUE)
            message(VERBOSE "Name resolution is provided by the C runtime")
        endif()
    endif()
endif()

mongo_pick(MONGOC_HAVE_DNSAPI 1 0 _MONGOC_HAVE_DNSAPI)
mongo_pick(MONGOC_HAVE_RES_NSEARCH 1 0 [[_MONGOC_HAVE_RES_NSEARCH_NOLINK OR _MONGOC_HAVE_RES_NSEARCH_RESOLV]])
mongo_pick(MONGOC_HAVE_RES_SEARCH 1 0 [[_MONGOC_HAVE_RES_SEARCH_NOLINK OR _MONGOC_HAVE_RES_SEARCH_RESOLV]])
mongo_pick(MONGOC_HAVE_RES_NDESTROY 1 0 [[_MONGOC_HAVE_RES_NDESTROY_NOLINK OR _MONGOC_HAVE_RES_NDESTROY_RESOLV]])
mongo_pick(MONGOC_HAVE_RES_NCLOSE 1 0 [[_MONGOC_HAVE_RES_NCLOSE_NOLINK OR _MONGOC_HAVE_RES_NCLOSE_RESOLV]])

if(RESOLVE_LIB_NAME OR resolve_is_libc)
    # Define the resolver interface:
    add_library(_mongoc-resolve INTERFACE)
    add_library(mongo::detail::c_resolve ALIAS _mongoc-resolve)
    set_target_properties(_mongoc-resolve PROPERTIES
        INTERFACE_LINK_LIBRARIES "${RESOLVE_LIB_NAME}"
        EXPORT_NAME detail::c_resolve)
    install(TARGETS _mongoc-resolve EXPORT mongoc-targets)
endif()

cmake_pop_check_state()
