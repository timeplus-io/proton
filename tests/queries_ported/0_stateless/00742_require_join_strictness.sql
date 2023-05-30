 
SET join_default_strictness = '';
SELECT * FROM system.one INNER JOIN (SELECT number AS k FROM system.numbers) as js2 ON dummy = k; -- { serverError 417 }
