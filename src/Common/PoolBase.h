#pragma once

#include <condition_variable>
#include <mutex>
#include <type_traits>
#include <variant>
#include <boost/noncopyable.hpp>
#include <Poco/Timespan.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>

/// proton: starts
#include <Common/Stopwatch.h>
/// proton: ends

/** A class from which you can inherit and get a pool of something. Used for database connection pools.
  * Descendant class must provide a method for creating a new object to place in the pool.
  */

template <typename TObject>
class PoolBase : private boost::noncopyable
{
public:
    using Object = TObject;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = std::shared_ptr<PoolBase<TObject>>;

    enum class BehaviourOnLimit
    {
        /**
         * Default behaviour - when limit on pool size is reached, callers will wait until object will be returned back in pool.
         */
        Wait,

        /**
         * If no free objects in pool - allocate a new object, but not store it in pool.
         * This behaviour is needed when we simply don't want to waste time waiting or if we cannot guarantee that query could be processed using fixed amount of connections.
         * For example, when we read from table on s3, one GetObject request corresponds to the whole FileSystemCache segment. This segments are shared between different
         * reading tasks, so in general case connection could be taken from pool by one task and returned back by another one. And these tasks are processed completely independently.
         */
        AllocateNewBypassingPool,
    };

private:

    /** The object with the flag, whether it is currently used. */
    struct PooledObject
    {
        PooledObject(ObjectPtr object_, PoolBase & pool_)
            : object(object_), pool(pool_)
        {
        }

        ObjectPtr object;
        bool in_use = false;
        std::atomic<bool> is_expired = false;
        PoolBase & pool;
    };

    using Objects = std::vector<std::shared_ptr<PooledObject>>;

    /** The helper, which sets the flag for using the object, and in the destructor - removes,
      *  and also notifies the event using condvar.
      */
    struct PoolEntryHelper
    {
        explicit PoolEntryHelper(PooledObject & data_) : data(data_) { data.in_use = true; }
        ~PoolEntryHelper()
        {
            std::lock_guard lock(data.pool.mutex);
            data.in_use = false;
            data.pool.available.notify_one();
        }

        PooledObject & data;
    };

public:
    /** What is given to the user. */
    class Entry
    {
    public:
        friend class PoolBase<Object>;

        Entry() = default;    /// For deferred initialization.

        /** The `Entry` object protects the resource from being used by another thread.
          * The following methods are forbidden for `rvalue`, so you can not write a similar to
          *
          * auto q = pool.get()->query("SELECT .."); // Oops, after this line Entry was destroyed
          * q.execute (); // Someone else can use this Connection
          */
        Object * operator->() && = delete;
        const Object * operator->() const && = delete;
        Object & operator*() && = delete;
        const Object & operator*() const && = delete;

        Object * operator->() &             { return castToObjectPtr(); }
        const Object * operator->() const & { return castToObjectPtr(); }
        Object & operator*() &              { return *castToObjectPtr(); }
        const Object & operator*() const &  { return *castToObjectPtr(); }

        /**
         * Expire an object to make it reallocated later.
         */
        void expire()
        {
            if (data.index() == 1)
                std::get<1>(data)->data.is_expired = true;
        }

        bool isNull() const { return data.index() == 0 ? !std::get<0>(data) : !std::get<1>(data); }

    private:
        /**
         * Plain object will be stored instead of PoolEntryHelper if fallback was made in get() (see BehaviourOnLimit::AllocateNewBypassingPool).
         */
        std::variant<ObjectPtr, std::shared_ptr<PoolEntryHelper>> data;

        explicit Entry(ObjectPtr && object) : data(std::move(object)) { }

        explicit Entry(PooledObject & object) : data(std::make_shared<PoolEntryHelper>(object)) { }

        auto castToObjectPtr() const
        {
            return std::visit(
                [](const auto & ptr)
                {
                    using T = std::decay_t<decltype(ptr)>;
                    if constexpr (std::is_same_v<ObjectPtr, T>)
                        return ptr.get();
                    else
                        return ptr->data.object.get();
                },
                data);
        }
    };

    virtual ~PoolBase() = default;

    /** Allocates the object.
     *  If 'behaviour_on_limit' is Wait - wait for free object in pool for 'timeout'. With 'timeout' < 0, the timeout is infinite.
     *  If 'behaviour_on_limit' is AllocateNewBypassingPool and there is no free object - a new object will be created but not stored in the pool.
     */
    /// proton: starts
    /// \param return_on_timeout - when is true, this function returns an uninitialized Entry if the wait times out.
    ///                            The caller should check if the returned entry is null or not before using it.
    /// proton: ends
    Entry get(Poco::Timespan::TimeDiff timeout, bool return_on_timeout=false)
    {
        std::unique_lock lock(mutex);

        while (true)
        {
            for (auto & item : items)
            {
                if (!item->in_use)
                {
                    if (likely(!item->is_expired))
                    {
                        return Entry(*item);
                    }
                    else
                    {
                        expireObject(item->object);
                        item->object = allocObject();
                        item->is_expired = false;
                        return Entry(*item);
                    }
                }
            }
            if (items.size() < max_items)
            {
                ObjectPtr object = allocObject();
                items.emplace_back(std::make_shared<PooledObject>(object, *this));
                return Entry(*items.back());
            }

            if (behaviour_on_limit == BehaviourOnLimit::AllocateNewBypassingPool)
                return Entry(allocObject());

            if (timeout < 0)
            {
                LOG_INFO(log, "No free connections in pool. Waiting indefinitely.");
                available.wait(lock);
            }
            else
            /// proton: starts
            {
                auto timeout_ms = std::chrono::milliseconds(timeout);
                LOG_INFO(log, "No free connections in pool. Waiting {} ms.", timeout_ms.count());
                auto status = available.wait_for(lock, timeout_ms);
                if (return_on_timeout && status == std::cv_status::timeout)
                    return {};
            }
            /// proton: ends
        }
    }

    void reserve(size_t count)
    {
        std::lock_guard lock(mutex);

        while (items.size() < count)
            items.emplace_back(std::make_shared<PooledObject>(allocObject(), *this));
    }

    inline size_t size()
    {
        std::lock_guard lock(mutex);
        return items.size();
    }

    /// proton: starts
    void waitForNoMoreInUse()
    {
        std::unique_lock lock(mutex);
        while (true)
        {
            bool all_freed = true;
            for (const auto & item : items)
            {
                if (item->in_use)
                {
                    all_freed = false;
                    break;
                }
            }
            if (all_freed)
                return;

            available.wait(lock);
        }
    }
    /// proton: ends

private:
    /** The maximum size of the pool. */
    unsigned max_items;

    BehaviourOnLimit behaviour_on_limit;

    /** Lock to access the pool. */
    std::mutex mutex;
    std::condition_variable available;

protected:

    /** Pool. */
    Objects items; /// proton: updated

    LoggerPtr log;

    PoolBase(unsigned max_items_, LoggerPtr log_, BehaviourOnLimit behaviour_on_limit_ = BehaviourOnLimit::Wait)
        : max_items(max_items_), behaviour_on_limit(behaviour_on_limit_), log(log_)
    {
        items.reserve(max_items);
    }

    /** Creates a new object to put into the pool. */
    virtual ObjectPtr allocObject() = 0;
    virtual void expireObject(ObjectPtr) {}
};
