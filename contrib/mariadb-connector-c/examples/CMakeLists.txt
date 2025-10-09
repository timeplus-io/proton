SET(EXAMPLE_FILES "mysql_affected_rows" 
                  "mysql_debug")

INCLUDE_DIRECTORIES(${CC_SOURCE_DIR}/include)

ENABLE_TESTING()

# this will be the main tests which saves output
# from example files
ADD_EXECUTABLE(test_output test_output.c)

FOREACH(EXAMPLE_FILE ${EXAMPLE_FILES})
  SET(XML_EXAMPLE_FILES $XML_EXAMPLE_FILES "examples/${EXAMPLE_FILE}.c")
  ADD_EXECUTABLE(${EXAMPLE_FILE} ${EXAMPLE_FILE}.c)
  TARGET_LINK_LIBRARIES(${EXAMPLE_FILE} mariadbclient)
  ADD_TEST(TEST_${EXAMPLE_FILE} ./${EXECUTABLE_OUTPUT_PATH}/test_output ./${EXAMPLE_FILE} ${CC_SOURCE_DIR}/examples/${EXAMPLE_FILE}.out ${CC_SOURCE_DIR}/examples/${EXAMPLE_FILE}.exp)

ENDFOREACH(EXAMPLE_FILE ${EXAMPLE_FILES})
