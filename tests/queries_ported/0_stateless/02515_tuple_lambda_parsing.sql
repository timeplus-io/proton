explain ast select tuple(a) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select tuple(a, b) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select (tuple(a)) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select (f(a)) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select (a::uint64) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select (1) -> f(a); -- { clientError SYNTAX_ERROR }
explain ast select (1::uint64) -> f(a); -- { clientError SYNTAX_ERROR }
