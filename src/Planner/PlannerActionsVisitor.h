#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

/// Calculate action node name for constant
String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type);

/// Calculate action node name for constant, data type will be derived from constant literal value
String calculateConstantActionNodeName(const Field & constant_literal);

}
