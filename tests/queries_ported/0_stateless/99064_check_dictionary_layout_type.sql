CREATE DICTIONARY test_99064 (ip string)
PRIMARY KEY ip
SOURCE(POSTGRESQL(HOST 'not.important' PORT 28851 USER 'user1' PASSWORD 'showmethemoney' TABLE 'internal_ip' DB 'defaultdb'))
LIFETIME(3600)
LAYOUT(FLAT()); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

