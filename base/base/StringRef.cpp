#include <ostream>

#include "StringRef.h"


std::ostream & operator<<(std::ostream & os, StringRef str)
{
    if (str.data)
        os.write(str.data, str.size);

    return os;
}

