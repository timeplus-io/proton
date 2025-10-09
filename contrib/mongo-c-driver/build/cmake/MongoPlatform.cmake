#[[

Defines a target mongo::detail::c_platform (alias of _mongo-platform), which
exposes system-level supporting compile and link usage requirements. All targets
should link to this target with level PUBLIC.

Use mongo_platform_compile_options and mongo_platform_link_options to add usage
requirements to this library.

The mongo::detail::c_platform library is installed and exported with the
bson-targets export set as an implementation detail. It is installed with this
export set so that it is available to both libbson and libmongoc (attempting to
install this target in both bson-targets and mongoc-targets export sets would
lead to duplicate definitions of mongo::detail::c_platform for downstream
users).

]]

add_library(_mongo-platform INTERFACE)
if (NOT USE_SYSTEM_LIBBSON)
    add_library(mongo::detail::c_platform ALIAS _mongo-platform)
else ()
   # The system libbson exports the `mongo::detail::c_platform` target.
   # Do not define the `mongo::detail::c_platform` target, to prevent an "already defined" error.
endif ()
set_property(TARGET _mongo-platform PROPERTY EXPORT_NAME detail::c_platform)
install(TARGETS _mongo-platform EXPORT bson-targets)


#[[
Define additional platform-support compile options

These options are added to the mongo::detail::c_platform INTERFACE library.
]]
function (mongo_platform_compile_options)
    list(APPEND CMAKE_MESSAGE_CONTEXT ${CMAKE_CURRENT_FUNCTION})
    message(DEBUG "Add platform-support compilation options: ${ARGN}")
    target_compile_options(_mongo-platform INTERFACE ${ARGN})
endfunction ()

#[[
Define additional platform-support link options.

These options are added to the mongo::detail::c_platform INTERFACE library.
]]
function(mongo_platform_link_options)
    list(APPEND CMAKE_MESSAGE_CONTEXT ${CMAKE_CURRENT_FUNCTION})
    message(DEBUG "Add platform-support runtime linking options: ${ARGN}")
    target_link_options(_mongo-platform INTERFACE ${ARGN})
endfunction()

#[[
Add targets to the usage requirements for the current platform.

All of the named items must be the names of existing targets. Note that these
targets will also need to be available at import-time for consumers (unless
wrapped in $<BUILD_INTERFACE:>).
]]
function(mongo_platform_use_target)
    list(APPEND CMAKE_MESSAGE_CONTEXT ${CMAKE_CURRENT_FUNCTION})
    message(DEBUG "Add platform-support usage of targets: ${ARGN}")
    foreach(item IN LISTS ARGN)
        if(item MATCHES "::")
            # CMake will enforce that this link names an existing target
            target_link_libraries(_mongo-platform INTERFACE "${item}")
        else()
            # Generate a configure-time-error if the named item is not the name of a target
            target_link_libraries(_mongo-platform INTERFACE
                $<IF:$<TARGET_EXISTS:${item}>,${item},NO_SUCH_TARGET::${item}>)
        endif()
    endforeach()
endfunction()

#[[
Add non-target link library as usage requirements for the current platform.

This is intended for adding libraries that need to be linked. To add targets
as usage requirements, use mongo_platform_use_target. For adding link options,
use mongo_platform_link_options.
]]
function(mongo_platform_link_libraries)
    list(APPEND CMAKE_MESSAGE_CONTEXT ${CMAKE_CURRENT_FUNCTION})
    foreach(item IN LISTS ARGN)
        message(DEBUG "Add platform-support link library: ${item}")
        target_link_libraries(_mongo-platform INTERFACE "${item}")
    endforeach()
endfunction()
