SELECT has_any([['Hello, world']], [[[]]]); -- { serverError 386 }
SELECT has_any([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
SELECT has_all([['Hello, world']], [['Hello', 'world'], ['Hello, world']]);
