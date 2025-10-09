#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
/// proton: starts
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
/// proton: ends


namespace DB
{

/** Return single row with single column "statement" of type String with text of query to CREATE specified table.
  */
class InterpreterShowCreateQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowCreateQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    Block getSampleBlock() const;

private:
    ASTPtr query_ptr;

    QueryPipeline executeImpl();

    /// proton : starts
    QueryPipeline showMultiVersions(const StorageID & table_id) const;
    /// proton : ends
};


}
