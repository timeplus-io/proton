#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/ActionLock.h>
#include <Disks/IVolume.h>


namespace Poco { class Logger; }

namespace DB
{

class Context;
class AccessRightsElements;
class ASTSystemQuery;


/** Implement various SYSTEM queries.
  * Examples: SYSTEM SHUTDOWN, SYSTEM DROP MARK CACHE.
  *
  * Some commands are intended to stop/start background actions for tables and comes with two variants:
  *
  * 1. SYSTEM STOP MERGES table, SYSTEM START MERGES table
  * - start/stop actions for specific table.
  *
  * 2. SYSTEM STOP MERGES, SYSTEM START MERGES
  * - start/stop actions for all existing tables.
  * Note that the actions for tables that will be created after this query will not be affected.
  */
class InterpreterSystemQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    LoggerPtr log = nullptr;
    StorageID table_id = StorageID::createEmpty();      /// Will be set up if query contains table name
    VolumePtr volume_ptr;

    /// proton: starts
    void executePauseStream(const ASTSystemQuery & system);
    void executeResumeStream(const ASTSystemQuery & system);
    void executeRecoverStream(const ASTSystemQuery & system);
    void doExecuteStreamAdmission(const ASTSystemQuery & system, std::string_view admission_action);
    void executeMaterializedViewAdmission(const ASTSystemQuery & system);
    void executeSetLogLevel(const ASTSystemQuery & system);
    BlockIO executeShowLoggers(const ASTSystemQuery & system);
    void executePauseTask(const ASTSystemQuery & system);
    void executeResumeTask(const ASTSystemQuery & system);
    void updateTaskStatus(String database, const String & task_name, uint32_t new_status);
    #if !USE_PYTHON_UDF
    [[noreturn]] void executeInstallPythonPackage(const ASTSystemQuery & system);
    [[noreturn]] void executeUninstallPythonPackage(const ASTSystemQuery & system);
    #else
    void executeInstallPythonPackage(const ASTSystemQuery & system);
    void executeUninstallPythonPackage(const ASTSystemQuery & system);
    #endif
    BlockIO executeListPythonPackages(const ASTSystemQuery & system);
    /// proton: ends

    void flushDistributed(ASTSystemQuery & query);
    [[noreturn]] void restartDisk(String & name);

    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    void startStopAction(StorageActionBlockType action_type, bool start);

    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const override;
};


}
