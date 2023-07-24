#pragma once

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <Formats/registerFormats.h>


inline void tryRegisterFunctions()
{
    static struct Register { Register() { DB::registerFunctions(); } } registered;
}

inline void tryRegisterFormats()
{
    static struct Register { Register() { DB::registerFormats(); } } registered;
}

inline void tryRegisterAggregateFunctions()
{
    static struct Register { Register() { DB::registerAggregateFunctions(); } } registered;
}