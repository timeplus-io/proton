find_package (Python3 COMPONENTS Interpreter)

if (NOT TARGET Python3::Interpreter)
    message (STATUS "Python3 was not found, so test fixtures will not be defined")
    return ()
endif ()

get_filename_component(_MONGOC_BUILD_SCRIPT_DIR "${CMAKE_CURRENT_LIST_DIR}" DIRECTORY)
set (_MONGOC_PROC_CTL_COMMAND "$<TARGET_FILE:Python3::Interpreter>" -u -- "${_MONGOC_BUILD_SCRIPT_DIR}/proc-ctl.py")


function (mongo_define_subprocess_fixture name)
    cmake_parse_arguments(PARSE_ARGV 1 ARG "" "SPAWN_WAIT;STOP_WAIT;WORKING_DIRECTORY" "COMMAND")
    string (MAKE_C_IDENTIFIER ident "${name}")
    if (NOT ARG_SPAWN_WAIT)
        set (ARG_SPAWN_WAIT 1)
    endif ()
    if (NOT ARG_STOP_WAIT)
        set (ARG_STOP_WAIT 5)
    endif ()
    if (NOT ARG_WORKING_DIRECTORY)
        set (ARG_WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    endif ()
    if (NOT ARG_COMMAND)
        message (SEND_ERROR "mongo_define_subprocess_fixture(${name}) requires a COMMAND")
        return ()
    endif ()
    get_filename_component (ctl_dir "${CMAKE_CURRENT_BINARY_DIR}/${ident}.ctl" ABSOLUTE)
    add_test (NAME "${name}/start"
              COMMAND ${_MONGOC_PROC_CTL_COMMAND} start
                "--ctl-dir=${ctl_dir}"
                "--cwd=${ARG_WORKING_DIRECTORY}"
                "--spawn-wait=${ARG_SPAWN_WAIT}"
                -- ${ARG_COMMAND})
    add_test (NAME "${name}/stop"
              COMMAND ${_MONGOC_PROC_CTL_COMMAND} stop "--ctl-dir=${ctl_dir}" --if-not-running=ignore)
    set_property (TEST "${name}/start" PROPERTY FIXTURES_SETUP "${name}")
    set_property (TEST "${name}/stop" PROPERTY FIXTURES_CLEANUP "${name}")
endfunction ()

# Create a fixture that runs a fake Azure IMDS server
mongo_define_subprocess_fixture(
    mongoc/fixtures/fake_imds
    SPAWN_WAIT 0.2
    COMMAND
        "$<TARGET_FILE:Python3::Interpreter>" -u --
        "${_MONGOC_BUILD_SCRIPT_DIR}/bottle.py" fake_kms_provider_server:kms_provider
            --bind localhost:14987  # Port 14987 chosen arbitrarily
    )
