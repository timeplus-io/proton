#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
public:
    /// Daisy : starts
    void setPipeMode(bool pipe_mode_) { pipe_mode = pipe_mode_; }
    /// Daisy : ends

protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    /// Daisy : starts
    bool pipe_mode = false;
    /// Daisy : ends
};

}
