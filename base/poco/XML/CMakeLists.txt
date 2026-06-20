# Poco::XML (expat)

file (GLOB SRCS_EXPAT src/xml*.c)

add_library (_poco_xml_expat ${SRCS_EXPAT})
add_library (Poco::XML::Expat ALIAS _poco_xml_expat)

# TODO: remove these warning exclusions
target_compile_options (_poco_xml_expat
    PRIVATE
        -Wno-cast-qual
        -Wno-empty-translation-unit
        -Wno-extra-semi-stmt
        -Wno-implicit-fallthrough
        -Wno-reserved-identifier
        -Wno-unused-macros
        -Wno-implicit-int-conversion
)

target_include_directories (_poco_xml_expat PUBLIC "include")
target_include_directories (_poco_xml_expat PRIVATE "../Foundation/include")

# pyexpat (a python standard library) depends on this library
if (ENABLE_PYTHON_UDF)
    target_include_directories (_poco_xml_expat PUBLIC "include/Poco/XML")
endif()

# Poco::XML

file (GLOB SRCS src/*.cpp)
add_library (_poco_xml ${SRCS})
add_library (Poco::XML ALIAS _poco_xml)
# proton: starts. ClickHouse sets XML_GE=0 here; we need XML_GE=1.
# CPython's pyexpat (>= 3.14.6, gh-90949) links the Billion-Laughs protection
# API (XML_SetBillionLaughsAttackProtection*), which expat only compiles when
# general entities are enabled (XML_GE=1). PUBLIC so the pyexpat extension,
# which links Poco::XML, also sees the declarations in expat.h. General-entity
# expansion matches the behavior of the expat 2.4.7 this fork shipped before
# the bump, and unlike 2.4.7 the expansion-accounting protection is now active
# by default (amplification <= 100, threshold 8 MiB).
target_compile_definitions (_poco_xml PUBLIC XML_GE=1)
# The XML_GE=1-only alloc tracker in xmlparse.cpp casts its char buffers back
# to XML_Parser; upstream compiles that file as C without -Wcast-align.
target_compile_options (_poco_xml PRIVATE -Wno-cast-align)
# proton: ends.

# TODO: remove these warning exclusions
target_compile_options (_poco_xml
    PRIVATE
        -Wno-c99-extensions
        -Wno-cast-qual
        -Wno-covered-switch-default
        -Wno-deprecated-dynamic-exception-spec
        -Wno-disabled-macro-expansion
        -Wno-implicit-fallthrough
        -Wno-missing-noreturn
        -Wno-old-style-cast
        -Wno-reserved-identifier
        -Wno-shadow
        -Wno-shorten-64-to-32
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-tautological-type-limit-compare
        -Wno-tautological-unsigned-zero-compare
        -Wno-unreachable-code
        -Wno-unreachable-code-break
        -Wno-unused-macros
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)
target_include_directories (_poco_xml SYSTEM PUBLIC "include")
target_link_libraries (_poco_xml PUBLIC Poco::Foundation Poco::XML::Expat)
