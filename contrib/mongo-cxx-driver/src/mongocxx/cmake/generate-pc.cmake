set(input_vars
    src_dir
    bin_dir
    prefix
    includedir
    libdir
    output_name
    version
    is_static
    bsoncxx_name
    libmongoc_req_abi_ver
    libmongoc_req_ver
)

foreach(var ${input_vars})
    if(NOT DEFINED "${var}")
        message(FATAL_ERROR "${var} was not set!")
    endif()
endforeach()

if(is_static)
    set(pkgname "libmongocxx-static")
else()
    set(pkgname "libmongocxx")
endif()

if(1)
    set(requires "")

    if(is_static)
        list(APPEND requires "lib${bsoncxx_name} >= ${version}")
        list(APPEND requires "libmongoc-static-${libmongoc_req_abi_ver} >= ${libmongoc_req_ver}")
    else()
        list(APPEND requires "lib${bsoncxx_name} >= ${version}")
    endif()

    list(JOIN requires ", " requires)
endif()

if(1)
    set(cflags "")

    if(is_static)
        list(APPEND cflags "-DMONGOCXX_STATIC")
    endif()

    list(APPEND cflags "-I\${includedir}/mongocxx/v_noabi")
    list(APPEND cflags "-I\${includedir}")

    list(JOIN cflags " " cflags)
endif()

configure_file(
    ${src_dir}/libmongocxx.pc.in
    ${bin_dir}/lib${output_name}.pc
    @ONLY
)
