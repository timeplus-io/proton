#pragma once

#include <Common/Exception.h>
#include <Common/NamePrompter.h>
#include <base/types.h>
#include <Poco/String.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** If stored objects may have several names (aliases)
  * this interface may be helpful
  * template parameter is available as Value
  */
template <typename ValueType>
class IFactoryWithAliases : public IHints<2, IFactoryWithAliases<ValueType>>
{
protected:
    using Value = ValueType;

    String getAliasToOrName(const String & name) const
    {
        if (aliases.contains(name))
            return aliases.at(name);
        else if (String name_lowercase = Poco::toLower(name); case_insensitive_aliases.contains(name_lowercase))
            return case_insensitive_aliases.at(name_lowercase);
        else
            return name;
    }

    /// proton: starts
    String getClickHouseAliasToOrName(const String & name) const
    {
        if (clickhouse_names.contains(name))
            return clickhouse_names.at(name);
        else
            return name;
    }

    String getClickHouseAliasFromOrName(const String & name) const
    {
        if (reversed_clickhouse_names.contains(name))
            return reversed_clickhouse_names.at(name);
        else
            return name;
    }
    /// proton: ends

    std::unordered_map<String, String> case_insensitive_name_mapping;

public:
    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /** Register additional name for value
      * real_name have to be already registered.
      */
    void registerAlias(const String & alias_name, const String & real_name, CaseSensitiveness case_sensitiveness = CaseInsensitive)
    {
        const auto & creator_map = getMap();
        const auto & case_insensitive_creator_map = getCaseInsensitiveMap();
        const String factory_name = getFactoryName();

        String real_dict_name;
        if (creator_map.count(real_name))
            real_dict_name = real_name;
        else if (auto real_name_lowercase = Poco::toLower(real_name); case_insensitive_creator_map.count(real_name_lowercase))
            real_dict_name = real_name_lowercase;
        else
            throw Exception(factory_name + ": can't create alias '" + alias_name + "', the real name '" + real_name + "' is not registered",
                ErrorCodes::LOGICAL_ERROR);

        String alias_name_lowercase = Poco::toLower(alias_name);

        if (creator_map.count(alias_name) || case_insensitive_creator_map.count(alias_name_lowercase))
            throw Exception(
                factory_name + ": the alias name '" + alias_name + "' is already registered as real name", ErrorCodes::LOGICAL_ERROR);

        if (case_sensitiveness == CaseInsensitive)
        {
            if (!case_insensitive_aliases.emplace(alias_name_lowercase, real_dict_name).second)
                throw Exception(
                    factory_name + ": case insensitive alias name '" + alias_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
            case_insensitive_name_mapping[alias_name_lowercase] = real_name;
        }

        if (!aliases.emplace(alias_name, real_dict_name).second)
            throw Exception(factory_name + ": alias name '" + alias_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
    }

    /// proton: starts
    /// Register the name used by ClickHouse for value
    /// real_name have to be already registered.
    void registerClickHouseAlias(const String & alias_name, const String & alias_or_real_name)
    {
        const auto & creator_map = getMap();
        const auto & case_insensitive_creator_map = getCaseInsensitiveMap();
        const String factory_name = getFactoryName();

        String real_dict_name;
        String real_name = alias_or_real_name;
        if (auto it = aliases.find(real_name); it != aliases.end())
            real_name = it->second;
        if (creator_map.count(real_name))
            real_dict_name = real_name;
        else if (auto real_name_lowercase = Poco::toLower(real_name); case_insensitive_creator_map.count(real_name_lowercase))
            real_dict_name = real_name_lowercase;
        else
            throw Exception(factory_name + ": can't create ClickHouse alias '" + alias_name + "', the real name '" + alias_or_real_name + "' is not registered",
                ErrorCodes::LOGICAL_ERROR);

        if (!clickhouse_names.emplace(alias_name, real_dict_name).second)
            throw Exception(factory_name + ": ClickHouse alias name '" + alias_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
        reversed_clickhouse_names.emplace(real_dict_name, alias_name);
    }
    /// proton: ends

    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(getMap().begin(), getMap().end(), std::back_inserter(result), getter);
        std::transform(aliases.begin(), aliases.end(), std::back_inserter(result), getter);
        return result;
    }

    bool isCaseInsensitive(const String & name) const
    {
        String name_lowercase = Poco::toLower(name);
        return getCaseInsensitiveMap().contains(name_lowercase) || case_insensitive_aliases.contains(name_lowercase);
    }

    const String & aliasTo(const String & name) const
    {
        if (auto it = aliases.find(name); it != aliases.end())
            return it->second;
        else if (auto jt = case_insensitive_aliases.find(Poco::toLower(name)); jt != case_insensitive_aliases.end())
            return jt->second;

        throw Exception(getFactoryName() + ": name '" + name + "' is not alias", ErrorCodes::LOGICAL_ERROR);
    }

    bool isAlias(const String & name) const
    {
        return aliases.contains(name) || case_insensitive_aliases.contains(name);
    }

    /// proton: starts
    bool hasBuiltInNameOrAlias(const String & name) const
    {
        return getMap().contains(name) || getCaseInsensitiveMap().contains(name) || isAlias(name);
    }

    virtual bool hasNameOrAlias(const String & name) const { return hasBuiltInNameOrAlias(name); }
    /// proton: ends

    /// Return the canonical name (the name used in registration) if it's different from `name`.
    const String & getCanonicalNameIfAny(const String & name) const
    {
        auto it = case_insensitive_name_mapping.find(Poco::toLower(name));
        if (it != case_insensitive_name_mapping.end())
            return it->second;
        return name;
    }

    virtual ~IFactoryWithAliases() override {}

private:
    using InnerMap = std::unordered_map<String, Value>; // name -> creator
    using AliasMap = std::unordered_map<String, String>; // alias -> original type

    virtual const InnerMap & getMap() const = 0;
    virtual const InnerMap & getCaseInsensitiveMap() const = 0;
    virtual String getFactoryName() const = 0;

    /// Alias map to data_types from previous two maps
    AliasMap aliases;

    /// Case insensitive aliases
    AliasMap case_insensitive_aliases;

    /// proton: starts
    /// ClickHouse names map to data_types from previous two maps
    AliasMap clickhouse_names;
    /// For looking up Proton type names from ClickHouse names
    AliasMap reversed_clickhouse_names;
    /// proton: ends
};

}
