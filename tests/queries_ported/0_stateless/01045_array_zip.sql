SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f']);

SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']);

SELECT array_zip(); -- { serverError 42 }

SELECT array_zip('a', 'b', 'c'); -- { serverError 43 }

SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']); -- { serverError 190 }
