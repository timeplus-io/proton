#include "SimpleJSON.h"

#include "find_symbols.h"
#include "preciseExp10.h"

#include <Poco/NumberParser.h>
#include <Poco/UTF8Encoding.h>

#include <string>
#include <string.h>


#include <iostream>

#define JSON_MAX_DEPTH 100


POCO_IMPLEMENT_EXCEPTION(SimpleJSONException, Poco::Exception, "SimpleJSONException")


/// Прочитать беззнаковое целое в простом формате из не-0-terminated строки.
static UInt64 readUIntText(const char * buf, const char * end)
{
    UInt64 x = 0;

    if (buf == end)
        throw SimpleJSONException("JSON: cannot parse unsigned integer: unexpected end of data.");

    while (buf != end)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                return x;
        }
        ++buf;
    }

    return x;
}


/// Прочитать знаковое целое в простом формате из не-0-terminated строки.
static Int64 readIntText(const char * buf, const char * end)
{
    bool negative = false;
    UInt64 x = 0;

    if (buf == end)
        throw SimpleJSONException("JSON: cannot parse signed integer: unexpected end of data.");

    bool run = true;
    while (buf != end && run)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                run = false;
                break;
        }
        ++buf;
    }

    return negative ? -x : x;
}


/// Прочитать число с плавающей запятой в простом формате, с грубым округлением, из не-0-terminated строки.
static double readFloatText(const char * buf, const char * end)
{
    bool negative = false;
    double x = 0;
    bool after_point = false;
    double power_of_ten = 1;

    if (buf == end)
        throw SimpleJSONException("JSON: cannot parse floating point number: unexpected end of data.");

    bool run = true;
    while (buf != end && run)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '.':
                after_point = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                if (after_point)
                {
                    power_of_ten /= 10;
                    x += (*buf - '0') * power_of_ten;
                }
                else
                {
                    x *= 10;
                    x += *buf - '0';
                }
                break;
            case 'e':
            case 'E': {
                ++buf;
                Int32 exponent = readIntText(buf, end);
                x *= preciseExp10(exponent);

                run = false;
                break;
            }
            default:
                run = false;
                break;
        }
        ++buf;
    }
    if (negative)
        x = -x;

    return x;
}


void SimpleJSON::checkInit() const
{
    if (ptr_begin >= ptr_end)
        throw SimpleJSONException("JSON: begin >= end.");

    if (level > JSON_MAX_DEPTH)
        throw SimpleJSONException("JSON: too deep.");
}


SimpleJSON::ElementType SimpleJSON::getType() const
{
    /// Daisy : starts
    Pos pos = skipWhitespaceIfAny();
    return getType(pos);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::ElementType SimpleJSON::getType(Pos pos) const
{
    switch (*pos)
    {
        case '{':
            return TYPE_OBJECT;
        case '[':
            return TYPE_ARRAY;
        case 't':
        case 'f':
            return TYPE_BOOL;
        case 'n':
            return TYPE_NULL;
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return TYPE_NUMBER;
        case '"': {
            /// Проверим - это просто строка или name-value pair
            Pos after_string = skipString(pos);
            after_string = skipWhitespaceIfAny(after_string);
            if (after_string < ptr_end && *after_string == ':')
                return TYPE_NAME_VALUE_PAIR;
            else
                return TYPE_STRING;
        }
        default:
            throw SimpleJSONException(std::string("JSON: unexpected char ") + *ptr_begin + ", expected one of '{[tfn-0123456789\"'");
    }
}
/// Daisy : ends

void SimpleJSON::checkPos(Pos pos) const
{
    if (pos >= ptr_end || ptr_begin == nullptr)
        throw SimpleJSONException("JSON: unexpected end of data.");
}


SimpleJSON::Pos SimpleJSON::skipString() const
{
    //std::cerr << "skipString()\t" << data() << std::endl;
    /// Dasiy : starts
    return skipString(ptr_begin);
    /// Daisy : ends
}

/// Dasiy : starts
SimpleJSON::Pos SimpleJSON::skipString(Pos start) const
{
    Pos pos = skipWhitespaceIfAny(start);
    checkPos(pos);
    if (*pos != '"')
        throw SimpleJSONException(std::string("JSON: expected \", got ") + *pos);
    ++pos;

    /// fast path: находим следующую двойную кавычку. Если перед ней нет бэкслеша - значит это конец строки (при допущении корректности JSON).
    Pos closing_quote = reinterpret_cast<const char *>(memchr(reinterpret_cast<const void *>(pos), '\"', ptr_end - pos));
    if (nullptr != closing_quote && closing_quote[-1] != '\\')
        return closing_quote + 1;

    /// slow path
    while (pos < ptr_end && *pos != '"')
    {
        if (*pos == '\\')
        {
            ++pos;
            checkPos(pos);
            if (*pos == 'u')
            {
                pos += 4;
                checkPos(pos);
            }
        }
        ++pos;
    }

    checkPos(pos);
    if (*pos != '"')
        throw SimpleJSONException(std::string("JSON: expected \", got ") + *pos);
    ++pos;

    return pos;
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipNumber() const
{
    /// Daisy : starts
    return skipNumber(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipNumber(Pos begin) const
{
    Pos pos = skipWhitespaceIfAny(begin);

    checkPos(pos);
    if (*pos == '-')
        ++pos;

    while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;
    if (pos < ptr_end && *pos == '.')
        ++pos;
    while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;
    if (pos < ptr_end && (*pos == 'e' || *pos == 'E'))
        ++pos;
    if (pos < ptr_end && *pos == '-')
        ++pos;
    while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;

    return pos;
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipBool() const
{
    //std::cerr << "skipBool()\t" << data() << std::endl;
    /// Daisy : starts
    return skipBool(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipBool(Pos begin) const
{
    Pos pos = skipWhitespaceIfAny(begin);
    checkPos(pos);

    if (*pos == 't')
        pos += 4;
    else if (*pos == 'f')
        pos += 5;
    else
        throw SimpleJSONException("JSON: expected true or false.");

    return pos;
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipNull() const
{
    //std::cerr << "skipNull()\t" << data() << std::endl;

    /// Daisy : starts
    return skipBool(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipNull(Pos begin) const
{
    return begin + 4;
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipNameValuePair() const
{
    /// Daisy : starts
    return skipNameValuePair(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipNameValuePair(Pos begin) const
{
    Pos pos = skipString(begin);
    checkPos(pos);

    pos = skipWhitespaceIfAny(pos);
    if (*pos != ':')
        throw SimpleJSONException("JSON: expected :.");
    ++pos;

    return SimpleJSON(pos, ptr_end, level + 1).skipElement();
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipArray() const
{
    //std::cerr << "skipArray()\t" << data() << std::endl;
    /// Daisy : starts
    return skipArray(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipArray(Pos begin) const
{
    //std::cerr << "skipArray()\t" << data() << std::endl;
    Pos pos = skipWhitespaceIfAny(begin);
    if (!isArray(pos))
        throw SimpleJSONException("JSON: expected [");

    ++pos;
    checkPos(pos);
    if (*pos == ']')
        return ++pos;

    while (true)
    {
        pos = SimpleJSON(pos, ptr_end, level + 1).skipElement();

        checkPos(pos);
        pos = skipWhitespaceIfAny(pos);
        switch (*pos)
        {
            case ',':
                ++pos;
                break;
            case ']':
                return ++pos;
            default:
                throw SimpleJSONException(std::string("JSON: expected one of ',]', got ") + *pos);
        }
    }
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipObject() const
{
    //std::cerr << "skipObject()\t" << data() << std::endl;
    /// Daisy : starts
    return skipObject(ptr_begin);
    /// Daisy : ends
}

/// Daisy : starts
SimpleJSON::Pos SimpleJSON::skipObject(const Pos begin) const
{
    Pos pos = skipWhitespaceIfAny(begin);
    if (!isObject(pos))
        throw SimpleJSONException("JSON: expected {");

    ++pos;
    checkPos(pos);
    if (*pos == '}')
        return ++pos;

    while (true)
    {
        pos = SimpleJSON(pos, ptr_end, level + 1).skipNameValuePair();

        checkPos(pos);
        pos = skipWhitespaceIfAny(pos);

        switch (*pos)
        {
            case ',':
                ++pos;
                break;
            case '}':
                return ++pos;
            default:
                throw SimpleJSONException(std::string("JSON: expected one of ',}', got ") + *pos);
        }
    }
}
/// Daisy : ends

SimpleJSON::Pos SimpleJSON::skipElement() const
{
    //std::cerr << "skipElement()\t" << data() << std::endl;
    /// Daisy : starts
    Pos pos = skipWhitespaceIfAny();
    ElementType type = getType(pos);

    switch (type)
    {
        case TYPE_NULL:
            return skipNull(pos);
        case TYPE_BOOL:
            return skipBool(pos);
        case TYPE_NUMBER:
            return skipNumber(pos);
        case TYPE_STRING:
            return skipString(pos);
        case TYPE_NAME_VALUE_PAIR:
            return skipNameValuePair(pos);
        case TYPE_ARRAY:
            return skipArray(pos);
        case TYPE_OBJECT:
            return skipObject(pos);
        default:
            throw SimpleJSONException("Logical error in JSON: unknown element type: " + std::to_string(type));
    }
    /// Daisy : ends
}

/// Daisy : starts
/// Skip whitespace characters.
SimpleJSON::Pos SimpleJSON::skipWhitespaceIfAny() const
{
    return skipWhitespaceIfAny(ptr_begin);
}

SimpleJSON::Pos SimpleJSON::skipWhitespaceIfAny(Pos begin) const
{
    Pos pos = begin;

    checkPos(pos);
    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r' || *pos == '\f' || *pos == '\v')
    {
        ++pos;
        checkPos(pos);
    }
    return pos;
}
/// Daisy : ends

size_t SimpleJSON::size() const
{
    size_t i = 0;

    for (const_iterator it = begin(); it != end(); ++it)
        ++i;

    return i;
}


bool SimpleJSON::empty() const
{
    return size() == 0;
}


SimpleJSON SimpleJSON::operator[](size_t n) const
{
    ElementType type = getType();

    if (type != TYPE_ARRAY)
        throw SimpleJSONException("JSON: not array when calling operator[](size_t) method.");

    Pos pos = ptr_begin;
    ++pos;
    checkPos(pos);

    size_t i = 0;
    const_iterator it = begin();
    while (i < n && it != end())
    {
        ++it;
        ++i;
    }

    if (i != n)
        throw SimpleJSONException("JSON: array index " + std::to_string(n) + " out of bounds.");

    return *it;
}


SimpleJSON::Pos SimpleJSON::searchField(const char * data, size_t size) const
{
    ElementType type = getType();

    if (type != TYPE_OBJECT)
        throw SimpleJSONException("JSON: not object when calling operator[](const char *) or has(const char *) method.");

    const_iterator it = begin();
    for (; it != end(); ++it)
    {
        if (!it->hasEscapes())
        {
            if (static_cast<int>(size) + 2 > it->dataEnd() - it->data())
                continue;
            if (!strncmp(data, it->data() + 1, size))
                break;
        }
        else
        {
            std::string current_name = it->getName();
            if (current_name.size() == size && 0 == memcmp(current_name.data(), data, size))
                break;
        }
    }

    if (it == end())
        return nullptr;
    else
        return it->data();
}


bool SimpleJSON::hasEscapes() const
{
    Pos pos = ptr_begin + 1;
    while (pos < ptr_end && *pos != '"' && *pos != '\\')
        ++pos;

    if (*pos == '"')
        return false;
    else if (*pos == '\\')
        return true;
    throw SimpleJSONException("JSON: unexpected end of data.");
}


bool SimpleJSON::hasSpecialChars() const
{
    Pos pos = ptr_begin + 1;
    while (pos < ptr_end && *pos != '"' && *pos != '\\' && *pos != '\r' && *pos != '\n' && *pos != '\t' && *pos != '\f' && *pos != '\b'
           && *pos != '\0' && *pos != '\'')
        ++pos;

    if (*pos == '"')
        return false;
    else if (pos < ptr_end)
        return true;
    throw SimpleJSONException("JSON: unexpected end of data.");
}


SimpleJSON SimpleJSON::operator[](const std::string & name) const
{
    Pos pos = searchField(name);
    if (!pos)
        throw SimpleJSONException("JSON: there is no element '" + std::string(name) + "' in object.");

    return SimpleJSON(pos, ptr_end, level + 1).getValue();
}


bool SimpleJSON::has(const char * data, size_t size) const
{
    return nullptr != searchField(data, size);
}


double SimpleJSON::getDouble() const
{
    return readFloatText(ptr_begin, ptr_end);
}

Int64 SimpleJSON::getInt() const
{
    return readIntText(ptr_begin, ptr_end);
}

UInt64 SimpleJSON::getUInt() const
{
    return readUIntText(ptr_begin, ptr_end);
}

bool SimpleJSON::getBool() const
{
    if (*ptr_begin == 't')
        return true;
    if (*ptr_begin == 'f')
        return false;
    throw SimpleJSONException("JSON: cannot parse boolean.");
}

std::string SimpleJSON::getString() const
{
    /// Daisy : starts
    Pos s = skipWhitespaceIfAny();
    /// Daisy : ends

    if (*s != '"')
        throw SimpleJSONException(std::string("JSON: expected \", got ") + *s);
    ++s;
    checkPos(s);

    std::string buf;
    do
    {
        Pos p = find_first_symbols<'\\', '"'>(s, ptr_end);
        if (p >= ptr_end)
        {
            break;
        }
        buf.append(s, p);
        s = p;
        switch (*s)
        {
            case '\\':
                ++s;
                checkPos(s);

                switch (*s)
                {
                    case '"':
                        buf += '"';
                        break;
                    case '\\':
                        buf += '\\';
                        break;
                    case '/':
                        buf += '/';
                        break;
                    case 'b':
                        buf += '\b';
                        break;
                    case 'f':
                        buf += '\f';
                        break;
                    case 'n':
                        buf += '\n';
                        break;
                    case 'r':
                        buf += '\r';
                        break;
                    case 't':
                        buf += '\t';
                        break;
                    case 'u': {
                        Poco::UTF8Encoding utf8;

                        ++s;
                        checkPos(s + 4);
                        std::string hex(s, 4);
                        s += 3;
                        int unicode;
                        try
                        {
                            unicode = Poco::NumberParser::parseHex(hex);
                        }
                        catch (const Poco::SyntaxException &)
                        {
                            throw SimpleJSONException("JSON: incorrect syntax: incorrect HEX code.");
                        }
                        buf.resize(buf.size() + 6); /// максимальный размер UTF8 многобайтовой последовательности
                        int res
                            = utf8.convert(unicode, reinterpret_cast<unsigned char *>(const_cast<char *>(buf.data())) + buf.size() - 6, 6);
                        if (!res)
                            throw SimpleJSONException("JSON: cannot convert unicode " + std::to_string(unicode) + " to UTF8.");
                        buf.resize(buf.size() - 6 + res);
                        break;
                    }
                    default:
                        buf += *s;
                        break;
                }
                ++s;
                break;
            case '"':
                return buf;
            default:
                throw SimpleJSONException("find_first_symbols<...>() failed in unexpected way");
        }
    } while (s < ptr_end);
    throw SimpleJSONException("JSON: incorrect syntax (expected end of string, found end of JSON).");
}

std::string SimpleJSON::getName() const
{
    return getString();
}

StringRef SimpleJSON::getRawString() const
{
    Pos s = ptr_begin;
    if (*s != '"')
        throw SimpleJSONException(std::string("JSON: expected \", got ") + *s);
    while (++s != ptr_end && *s != '"')
        ;
    if (s != ptr_end)
        return StringRef(ptr_begin + 1, s - ptr_begin - 1);
    throw SimpleJSONException("JSON: incorrect syntax (expected end of string, found end of JSON).");
}

StringRef SimpleJSON::getRawName() const
{
    return getRawString();
}

SimpleJSON SimpleJSON::getValue() const
{
    Pos pos = skipString();
    checkPos(pos);
    /// Daisy : starts
    pos = skipWhitespaceIfAny(pos);
    if (*pos != ':')
        throw SimpleJSONException("JSON: expected :.");
    ++pos;
    checkPos(pos);
    pos = skipWhitespaceIfAny(pos);
    /// Daisy : ends
    return SimpleJSON(pos, ptr_end, level + 1);
}


double SimpleJSON::toDouble() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getDouble();
    else if (type == TYPE_STRING)
        return SimpleJSON(ptr_begin + 1, ptr_end, level + 1).getDouble();
    else
        throw SimpleJSONException("JSON: cannot convert value to double.");
}

Int64 SimpleJSON::toInt() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getInt();
    else if (type == TYPE_STRING)
        return SimpleJSON(ptr_begin + 1, ptr_end, level + 1).getInt();
    else
        throw SimpleJSONException("JSON: cannot convert value to signed integer.");
}

UInt64 SimpleJSON::toUInt() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getUInt();
    else if (type == TYPE_STRING)
        return SimpleJSON(ptr_begin + 1, ptr_end, level + 1).getUInt();
    else
        throw SimpleJSONException("JSON: cannot convert value to unsigned integer.");
}

std::string SimpleJSON::toString() const
{
    ElementType type = getType();

    if (type == TYPE_STRING)
        return getString();
    else
    {
        Pos pos = skipElement();
        return std::string(ptr_begin, pos - ptr_begin);
    }
}


SimpleJSON::iterator SimpleJSON::iterator::begin() const
{
    /// Daisy : starts
    Pos pos = skipWhitespaceIfAny();
    ElementType type = getType(pos);
    /// Daisy : ends

    if (type != TYPE_ARRAY && type != TYPE_OBJECT)
        throw SimpleJSONException("JSON: not array or object when calling begin() method.");

    //std::cerr << "begin()\t" << data() << std::endl;
    /// Daisy : starts
    ++pos;
    /// Daisy : ends
    checkPos(pos);
    if (*pos == '}' || *pos == ']')
        return end();

    return SimpleJSON(pos, ptr_end, level + 1);
}

SimpleJSON::iterator SimpleJSON::iterator::end() const
{
    return SimpleJSON(nullptr, ptr_end, level + 1);
}

SimpleJSON::iterator & SimpleJSON::iterator::operator++()
{
    /// Daisy : starts
    ptr_begin = skipWhitespaceIfAny();
    Pos pos = skipElement();
    /// Daisy : ends

    checkPos(pos);

    if (*pos != ',')
        ptr_begin = nullptr;
    else
    {
        ++pos;
        checkPos(pos);
        ptr_begin = pos;
    }

    return *this;
}

SimpleJSON::iterator SimpleJSON::iterator::operator++(int) // NOLINT
{
    iterator copy(*this);
    ++*this;
    return copy;
}

template <>
double SimpleJSON::get<double>() const
{
    return getDouble();
}

template <>
std::string SimpleJSON::get<std::string>() const
{
    return getString();
}

template <>
Int64 SimpleJSON::get<Int64>() const
{
    return getInt();
}

template <>
UInt64 SimpleJSON::get<UInt64>() const
{
    return getUInt();
}

template <>
bool SimpleJSON::get<bool>() const
{
    return getBool();
}

template <>
bool SimpleJSON::isType<std::string>() const
{
    return isString();
}

template <>
bool SimpleJSON::isType<UInt64>() const
{
    return isNumber();
}

template <>
bool SimpleJSON::isType<Int64>() const
{
    return isNumber();
}

template <>
bool SimpleJSON::isType<bool>() const
{
    return isBool();
}
