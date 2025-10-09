-- Tags: no-fasttest
-- Needs rapidjson library
SELECT json_merge_patch(REPEAT('{"c":', 1000000)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 100000)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 10000)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 1000)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 100)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 10)); -- { serverError BAD_ARGUMENTS }
SELECT json_merge_patch(REPEAT('{"c":', 1)); -- { serverError BAD_ARGUMENTS }
