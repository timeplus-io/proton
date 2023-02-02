/** NOTE: The behaviour of substring and substring_utf8 is inconsistent when negative offset is greater than string size:
  * substring:
  *      hello
  * ^-----^ - offset -10, length 7, result: "he"
  * substring_utf8:
  *      hello
  *      ^-----^ - offset -10, length 7, result: "hello"
  * This may be subject for change.
  */
SELECT substring_utf8('hello, Ð¿Ñ�Ð¸Ð²ÐµÑ�', -9223372036854775808, number) FROM numbers(16) FORMAT Null;
