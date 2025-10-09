DROP FUNCTION unknown_function; -- {serverError 46}
DROP FUNCTION unknown_function SETTINGS force = true;

SET force=true;
DROP FUNCTION unknown_function;

SET force=false;
DROP FUNCTION unknown_function; -- {serverError 46}
