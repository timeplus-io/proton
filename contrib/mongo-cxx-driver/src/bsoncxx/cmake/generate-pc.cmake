set(input_vars
    src_dir
    bin_dir
    prefix
    includedir
    libdir
    output_name
    version
    is_static
    libbson_req_abi_ver
    libbson_req_ver
    mnmlstc_enabled
)

foreach(var ${input_vars})
    if(NOT DEFINED "${var}")
        message(FATAL_ERROR "${var} was not set!")
    endif()
endforeach()

if(is_static)
    set(pkgname "libbsoncxx-static")
else()
    set(pkgname "libbsoncxx")
endif()

if(1)
    set(requires "")

    if(is_static)
        list(APPEND requires "libbson-static-${libbson_req_abi_ver} >= ${libbson_req_ver}")
    endif()

    list(JOIN requires ", " requires)
endif()

if(1)
    set(cflags "")

    if(is_static)
        list(APPEND cflags "-DBSONCXX_STATIC")
    endif()

    if(mnmlstc_enabled)
        list(APPEND cflags "-I\${includedir}/bsoncxx/v_noabi/bsoncxx/third_party/mnmlstc")
    endif()

    list(APPEND cflags "-I\${includedir}/bsoncxx/v_noabi")
    list(APPEND cflags "-I\${includedir}")

    list(JOIN cflags " " cflags)
endif()

configure_file(
    ${src_dir}/libbsoncxx.pc.in
    ${bin_dir}/lib${output_name}.pc
    @ONLY
)
