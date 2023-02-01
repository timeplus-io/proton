CREATE DICTIONARY testip
(
    `network` string, 
    `test_field` string
)
PRIMARY KEY network
SOURCE(FILE(PATH '/tmp/test.csv' FORMAT CSVWithNames))
LIFETIME(MIN 0 MAX 300)
LAYOUT(IPTRIE()); -- { serverError 137 }
