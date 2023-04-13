-- Tags: no-fasttest

select throw_if(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
