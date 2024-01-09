#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

class ASTCreateExternalTableQuery : public ASTQueryWithTableAndOutput
{
public:
    bool create_or_replace {false};
    bool if_not_exists {false};

    ASTSetQuery * settings;

    String getID(char delim) const override { return "CreateExternalTableQuery" + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatQueryImpl(const FormatSettings & fmt_settings, FormatState & state, FormatStateStacked frame) const override;
};

}
