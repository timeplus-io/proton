
// Generated from ClickHouseLexer.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"


namespace DB {


class  ClickHouseLexer : public antlr4::Lexer {
public:
  enum {
    ADD = 1, AFTER = 2, ALIAS = 3, ALL = 4, ALTER = 5, AND = 6, ANTI = 7, 
    ANY = 8, ARRAY = 9, AS = 10, ASCENDING = 11, ASOF = 12, ASYNC = 13, 
    ATTACH = 14, BETWEEN = 15, BOTH = 16, BY = 17, CASE = 18, CAST = 19, 
    CHECK = 20, CLEAR = 21, CLUSTER = 22, CODEC = 23, COLLATE = 24, COLUMN = 25, 
    COMMENT = 26, CONSTRAINT = 27, CREATE = 28, CROSS = 29, CUBE = 30, DATABASE = 31, 
    DATABASES = 32, DATE = 33, DAY = 34, DEDUPLICATE = 35, DEFAULT = 36, 
    DELAY = 37, DELETE = 38, DESC = 39, DESCENDING = 40, DESCRIBE = 41, 
    DETACH = 42, DICTIONARIES = 43, DICTIONARY = 44, DISK = 45, DISTINCT = 46, 
    DISTRIBUTED = 47, DROP = 48, ELSE = 49, END = 50, ENGINE = 51, EVENTS = 52, 
    EXISTS = 53, EXPLAIN = 54, EXPRESSION = 55, EXTRACT = 56, FETCHES = 57, 
    FINAL = 58, FIRST = 59, FLUSH = 60, FOR = 61, FORMAT = 62, FREEZE = 63, 
    FROM = 64, FULL = 65, FUNCTION = 66, GLOBAL = 67, GRANULARITY = 68, 
    GROUP = 69, HAVING = 70, HIERARCHICAL = 71, HOUR = 72, ID = 73, IF = 74, 
    ILIKE = 75, IN = 76, INDEX = 77, INF = 78, INJECTIVE = 79, INNER = 80, 
    INSERT = 81, INTERVAL = 82, INTO = 83, IS = 84, IS_OBJECT_ID = 85, JOIN = 86, 
    KEY = 87, KILL = 88, LAST = 89, LAYOUT = 90, LEADING = 91, LEFT = 92, 
    LIFETIME = 93, LIKE = 94, LIMIT = 95, LIVE = 96, LOCAL = 97, LOGS = 98, 
    MATERIALIZED = 99, MAX = 100, MERGES = 101, MIN = 102, MINUTE = 103, 
    MODIFY = 104, MONTH = 105, MOVE = 106, MUTATION = 107, NAN_SQL = 108, 
    NO = 109, NOT = 110, NULL_SQL = 111, NULLS = 112, OFFSET = 113, ON = 114, 
    OPTIMIZE = 115, OR = 116, ORDER = 117, OUTER = 118, OUTFILE = 119, PARTITION = 120, 
    POPULATE = 121, PREWHERE = 122, PRIMARY = 123, QUARTER = 124, RANGE = 125, 
    RELOAD = 126, REMOVE = 127, RENAME = 128, REPLACE = 129, REPLICA = 130, 
    REPLICATED = 131, RIGHT = 132, ROLLUP = 133, SAMPLE = 134, SECOND = 135, 
    SELECT = 136, SEMI = 137, SENDS = 138, SET = 139, SETTINGS = 140, SHOW = 141, 
    SOURCE = 142, START = 143, STOP = 144, STREAM = 145, SUBSTRING = 146, 
    SYNC = 147, SYNTAX = 148, SYSTEM = 149, TABLE = 150, TABLES = 151, TEMPORARY = 152, 
    TEST = 153, THEN = 154, TIES = 155, TIMEOUT = 156, TIMESTAMP = 157, 
    TO = 158, TOP = 159, TOTALS = 160, TRAILING = 161, TRIM = 162, TRUNCATE = 163, 
    TTL = 164, TYPE = 165, UNION = 166, UPDATE = 167, USE = 168, USING = 169, 
    UUID = 170, VALUES = 171, VIEW = 172, VOLUME = 173, WATCH = 174, WEEK = 175, 
    WHEN = 176, WHERE = 177, WITH = 178, YEAR = 179, JSON_FALSE = 180, JSON_TRUE = 181, 
    IDENTIFIER = 182, FLOATING_LITERAL = 183, OCTAL_LITERAL = 184, DECIMAL_LITERAL = 185, 
    HEXADECIMAL_LITERAL = 186, STRING_LITERAL = 187, ARROW = 188, ASTERISK = 189, 
    BACKQUOTE = 190, BACKSLASH = 191, COLON = 192, COMMA = 193, CONCAT = 194, 
    DASH = 195, DOT = 196, EQ_DOUBLE = 197, EQ_SINGLE = 198, GE = 199, GT = 200, 
    LBRACE = 201, LBRACKET = 202, LE = 203, LPAREN = 204, LT = 205, NOT_EQ = 206, 
    PERCENT = 207, PLUS = 208, QUERY = 209, QUOTE_DOUBLE = 210, QUOTE_SINGLE = 211, 
    RBRACE = 212, RBRACKET = 213, RPAREN = 214, SEMICOLON = 215, SLASH = 216, 
    UNDERSCORE = 217, MULTI_LINE_COMMENT = 218, SINGLE_LINE_COMMENT = 219, 
    WHITESPACE = 220
  };

  ClickHouseLexer(antlr4::CharStream *input);
  ~ClickHouseLexer() override;

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace DB
