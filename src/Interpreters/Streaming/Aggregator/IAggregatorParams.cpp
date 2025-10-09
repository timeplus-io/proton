#include <DataTypes/DataTypeAggregateFunction.h>
#include <IO/Operators.h>
#include <Interpreters/Streaming/Aggregator/IAggregatorParams.h>
#include <Common/JSONBuilder.h>

namespace DB::Streaming
{

Block IAggregatorParams::getHeader(const Block & header_, const Names & keys_, const AggregateDescriptions & aggregates_, bool final_)
{
    Block res;

    for (const auto & key : keys_)
        res.insert(header_.getByName(key).cloneEmpty());

    for (const auto & aggregate : aggregates_)
    {
        size_t arguments_size = aggregate.argument_names.size();
        DataTypes argument_types(arguments_size);
        for (size_t j = 0; j < arguments_size; ++j)
            argument_types[j] = header_.getByName(aggregate.argument_names[j]).type;

        DataTypePtr type = final_ ? aggregate.function->getResultType()
                                  : std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);
        res.insert({std::move(type), aggregate.column_name});
    }

    return materializeBlock(res);
}

void IAggregatorParams::explain(WriteBuffer & out, size_t indent) const
{
    Strings res;
    String prefix(indent, ' ');

    {
        /// Dump keys.
        out << prefix << "Keys: ";

        bool first = true;
        for (const auto & key : keys)
        {
            if (!first)
                out << ", ";
            first = false;

            out << key;
        }

        out << '\n';
    }

    if (!aggregates.empty())
    {
        out << prefix << aggregatorName() << ":\n";

        for (const auto & aggregate : aggregates)
            aggregate.explain(out, indent + 4);
    }
}

void IAggregatorParams::explain(JSONBuilder::JSONMap & map) const
{
    auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & key : keys)
        keys_array->add(key);

    map.add("Keys", std::move(keys_array));

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & aggregate : aggregates)
        {
            auto aggregate_map = std::make_unique<JSONBuilder::JSONMap>();
            aggregate.explain(*aggregate_map);
            aggregates_array->add(std::move(aggregate_map));
        }

        map.add(std::string{aggregatorName()}, std::move(aggregates_array));
    }
}

}
