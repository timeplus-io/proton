#pragma once

#include "config.h"

#if USE_NLP

#include <mutex>
#include <string_view>
#include <unordered_map>

#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

/// FrequencyHolder class is responsible for storing and loading dictionaries
/// needed for text classification functions:
///
/// 1. detectLanguageUnknown
/// 2. detectCharset
/// 3. detectTonality
/// 4. detectProgrammingLanguage
///
/// The underlying .zst frequency tables are embedded directly into the binary
/// via C23 `#embed` in FrequencyHolder.cpp; no runtime file access.
class FrequencyHolder
{
public:
    /// proton: starts.
    struct Language
    {
        String name;
        HashMap<StringRef, Float64> map;
    };
    /// proton: ends.

    struct Encoding
    {
        String name;
        String lang;
        HashMap<UInt16, Float64> map;
    };

    using Map = HashMap<StringRef, Float64>;
    /// proton: starts.
    using Container = std::vector<Language>;
    /// proton: ends.
    using EncodingMap = HashMap<UInt16, Float64>;
    using EncodingContainer = std::vector<Encoding>;

    static FrequencyHolder & getInstance()
    {
        static FrequencyHolder instance;
        return instance;
    }

    const Map & getEmotionalDict()
    {
        std::lock_guard lock(mutex);
        if (emotional_dict.empty())
            loadEmotionalDict();
        return emotional_dict;
    }

    const EncodingContainer & getEncodingsFrequency()
    {
        std::lock_guard lock(mutex);
        if (encodings_freq.empty())
            loadEncodingsFrequency();
        return encodings_freq;
    }

    /// proton: starts.
    const Container & getProgrammingFrequency()
    {
        std::lock_guard lock(mutex);
        if (programming_freq.empty())
            loadProgrammingFrequency();
        return programming_freq;
    }
    /// proton: ends.

private:
    void loadEncodingsFrequency();
    void loadEmotionalDict();
    /// proton: starts.
    void loadProgrammingFrequency();
    /// proton: ends.

    Arena string_pool;

    Map emotional_dict;
    /// proton: starts.
    Container programming_freq;
    /// proton: ends.
    EncodingContainer encodings_freq;

    std::mutex mutex;
};

}

#endif
