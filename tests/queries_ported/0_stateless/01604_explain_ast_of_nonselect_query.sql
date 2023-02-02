explain ast; -- { clientError 62 }
explain ast alter stream t1 delete where date = today();
explain ast create function double AS  (n) -> 2*n;
