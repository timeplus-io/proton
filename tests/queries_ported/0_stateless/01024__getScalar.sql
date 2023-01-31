create stream foo (key string, macro string MATERIALIZED __get_scalar(key)) Engine=Null(); -- { serverError 43 }
