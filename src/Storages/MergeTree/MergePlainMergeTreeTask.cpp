#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ProfileEvents.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}


StorageID MergePlainMergeTreeTask::getStorageID()
{
    return storage.getStorageID();
}

void MergePlainMergeTreeTask::onCompleted()
{
    bool delay = (state == State::SUCCESS) || force_delay;
    task_result_callback(delay);
}

bool MergePlainMergeTreeTask::executeStep()
{
    /// All metrics will be saved in the thread_group, including all scheduled tasks.
    /// In profile_counters only metrics from this thread will be saved.
    ProfileEventsScope profile_events_scope(&profile_counters);

    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
    {
        switcher.emplace((*merge_list_entry)->thread_group, "", /*allow_existing_group*/ true);
    }

    switch (state)
    {
        case State::NEED_PREPARE :
        {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE :
        {
            try
            {
                if (merge_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            /// proton : starts.
            catch (DB::Exception & e)
            {
                /// issue-973, when memory limit is hit, we shall not retry merge anymore. Flag it and rethrow,
                /// the `onCompleted()` logic will try merge next time
                e.addMessage(storage.getName());

                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    force_delay = true;

                write_part_log(ExecutionStatus::fromCurrentException());
                throw;
            }
            /// proton : ends
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception is in merge_task.");
                write_part_log(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
        }
        case State::NEED_FINISH :
        {
            finish();

            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }
    return false;
}


void MergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        task_context);

    /// Emit a MERGE_PARTS_START at the very start so duration is measurable end-to-end
    /// from system.part_log alone (a paired MERGE_PARTS row will land at finalization).
    /// `new_part` is still null at this point — writePartLog falls back to source_parts
    /// for disk/path columns.
    storage.writePartLog(
        PartLogElement::MERGE_PARTS_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {});

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot));
    };

    transfer_profile_counters_to_initial_query = [this, query_thread_group = CurrentThread::getGroup()] ()
    {
        if (query_thread_group)
        {
            auto task_thread_group = (*merge_list_entry)->thread_group;
            auto task_counters_snapshot = task_thread_group->performance_counters.getPartiallyAtomicSnapshot();

            auto & query_counters = query_thread_group->performance_counters;
            for (ProfileEvents::Event i = ProfileEvents::Event(0); i < ProfileEvents::end(); ++i)
                query_counters.incrementNoTrace(i, task_counters_snapshot[i]);
        }
    };

    merge_task = storage.merger_mutator->mergePartsToTemporaryPart(
            future_part,
            metadata_snapshot,
            merge_list_entry.get(),
            {} /* projection_merge_list_element */,
            table_lock_holder,
            time(nullptr),
            task_context,
            merge_mutate_entry->tagger->reserved_space,
            deduplicate,
            deduplicate_by_columns,
            storage.merging_params,
            txn);
}


void MergePlainMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();

    MergeTreeData::Transaction transaction(storage, txn.get());
    storage.merger_mutator->renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction);
    transaction.commit();

    write_part_log({});
    StorageMergeTree::incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();
}

void MergePlainMergeTreeTask::cancel() noexcept
{
    if (merge_task)
        merge_task->cancel();
}

ContextMutablePtr MergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext());
    context->makeQueryContext();
    auto query_id = storage.getStorageID().getShortName() + "::" + future_part->name;
    context->setCurrentQueryId(query_id);
    return context;
}

}
