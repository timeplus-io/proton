create stream foo (key string, macro string MATERIALIZED __getScalar(key)) Engine=Null(); -- { serverError 43 }
