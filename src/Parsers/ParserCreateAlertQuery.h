#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE ALERT default.my_alert
/// BATCH 5 EVENTS WITH TIMEOUT 3 SECONDS
/// LIMIT 1 PER 5 SECOND
/// CALL send_to_pagerduty
/// AS SELECT * FROM bad_mat_views
class ParserCreateAlertQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE ALERT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
