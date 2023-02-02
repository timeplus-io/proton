SELECT extract_groups('hello', ''); -- { serverError 36 }
SELECT extract_all_groups('hello', ''); -- { serverError 36 }

SELECT extract_groups('hello', ' '); -- { serverError 36 }
SELECT extract_all_groups('hello', ' '); -- { serverError 36 }

SELECT extract_groups('hello', '\0'); -- { serverError 36 }
SELECT extract_all_groups('hello', '\0'); -- { serverError 36 }

SELECT extract_groups('hello', 'world'); -- { serverError 36 }
SELECT extract_all_groups('hello', 'world'); -- { serverError 36 }

SELECT extract_groups('hello', 'hello|world'); -- { serverError 36 }
SELECT extract_all_groups('hello', 'hello|world'); -- { serverError 36 }
