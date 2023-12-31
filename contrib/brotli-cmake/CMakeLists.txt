option (ENABLE_BROTLI "Enable brotli" ${ENABLE_LIBRARIES})

if (NOT ENABLE_BROTLI)
    message(STATUS "Not using brotli")
    return()
endif()

set(BROTLI_SOURCE_DIR "${proton_SOURCE_DIR}/contrib/brotli/c")
set(BROTLI_BINARY_DIR "${proton_BINARY_DIR}/contrib/brotli/c")

set(SRCS
    "${BROTLI_SOURCE_DIR}/enc/command.c"
    "${BROTLI_SOURCE_DIR}/enc/fast_log.c"
    "${BROTLI_SOURCE_DIR}/dec/bit_reader.c"
    "${BROTLI_SOURCE_DIR}/dec/state.c"
    "${BROTLI_SOURCE_DIR}/dec/huffman.c"
    "${BROTLI_SOURCE_DIR}/dec/decode.c"
    "${BROTLI_SOURCE_DIR}/enc/encode.c"
    "${BROTLI_SOURCE_DIR}/enc/dictionary_hash.c"
    "${BROTLI_SOURCE_DIR}/enc/cluster.c"
    "${BROTLI_SOURCE_DIR}/enc/entropy_encode.c"
    "${BROTLI_SOURCE_DIR}/enc/literal_cost.c"
    "${BROTLI_SOURCE_DIR}/enc/compress_fragment_two_pass.c"
    "${BROTLI_SOURCE_DIR}/enc/static_dict.c"
    "${BROTLI_SOURCE_DIR}/enc/compress_fragment.c"
    "${BROTLI_SOURCE_DIR}/enc/block_splitter.c"
    "${BROTLI_SOURCE_DIR}/enc/backward_references_hq.c"
    "${BROTLI_SOURCE_DIR}/enc/histogram.c"
    "${BROTLI_SOURCE_DIR}/enc/brotli_bit_stream.c"
    "${BROTLI_SOURCE_DIR}/enc/utf8_util.c"
    "${BROTLI_SOURCE_DIR}/enc/encoder_dict.c"
    "${BROTLI_SOURCE_DIR}/enc/backward_references.c"
    "${BROTLI_SOURCE_DIR}/enc/bit_cost.c"
    "${BROTLI_SOURCE_DIR}/enc/metablock.c"
    "${BROTLI_SOURCE_DIR}/enc/memory.c"
    "${BROTLI_SOURCE_DIR}/common/dictionary.c"
    "${BROTLI_SOURCE_DIR}/common/transform.c"
    "${BROTLI_SOURCE_DIR}/common/platform.c"
    "${BROTLI_SOURCE_DIR}/common/context.c"
    "${BROTLI_SOURCE_DIR}/common/constants.c"
)

add_library(_brotli ${SRCS})
add_library(ch_contrib::brotli ALIAS _brotli)

target_include_directories(_brotli SYSTEM BEFORE PUBLIC "${BROTLI_SOURCE_DIR}/include")

target_compile_definitions(_brotli PRIVATE BROTLI_BUILD_PORTABLE=1)
