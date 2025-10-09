#pragma once

namespace DB
{
/// @brief Public wrapper for a hybrid hash table mapping value.
struct HybridMappedValue
{
    struct ControlBits
    {
        /// If the entry has been modified since the last spill to persistent storage, `true` if modified, `false` otherwise.
        bool changed : 1 {false};

        /// (Unused) if the entry was loaded from persistent storage, `true` if loaded, `false` otherwise.
        bool loaded : 1 {false};

        uint8_t unused : 6 {0};
    };

    HybridMappedValue() = default;
    HybridMappedValue(void * data_, ControlBits & control_) : data(data_), control(&control_) { }

    ALWAYS_INLINE bool isValid() const noexcept { return data != nullptr; }

    ALWAYS_INLINE const void * getMapped() const noexcept { return data; }

    /// The change protocol is whenever `getMutableMapped()` is called by client, client is trying to update the 
    /// value of the key otherwise client shall use the const version `getMapped` otherwise it will mess up with 
    /// the tracking of the changed keys which ends up with unnecessary key / value serialization for incremental 
    /// ckpt cases where only changed keys / values need to be serialized.
    ALWAYS_INLINE void * getMutableMapped() noexcept
    {
        chassert(control);
        control->changed = true;
        return data;
    }

    ALWAYS_INLINE bool isDirty() const noexcept
    {
        chassert(control);
        return control->changed;
    }

    /// For internal use only by the hybrid hash table
    ALWAYS_INLINE void setData(void * data_) noexcept
    {
        chassert(control);
        data = data_;
    }

protected:
    /// The lifetime of the mapped value (\data_ and \control) is short-lived:
    /// - For batch interfaces like `HybridHashTable::forBatchKeyValue()`, it is valid until the end of the batch.
    /// - Otherwise, it is only valid during the current operation.
    void * data;
    ControlBits * control;
};
}
