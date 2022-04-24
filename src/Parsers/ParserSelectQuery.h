#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
public:
    /// proton: starts
    void setPipeMode(bool pipe_mode_) { pipe_mode = pipe_mode_; }
    /// proton: ends

protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;

private:
    /// proton: starts
    bool pipe_mode = false;
    /// proton: ends
};

}
