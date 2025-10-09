#include "DictionaryFactory.h"
#include "DictionarySourceFactory.h"

namespace DB
{

class DictionarySourceFactory;

void registerDictionarySourceNull(DictionarySourceFactory & factory);
void registerDictionarySourceFile(DictionarySourceFactory & source_factory);
void registerDictionarySourceMysql(DictionarySourceFactory & source_factory);
void registerDictionarySourceTimeplus(DictionarySourceFactory & source_factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & source_factory);
void registerDictionarySourcePostgreSQL(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutable(DictionarySourceFactory & source_factory);
void registerDictionarySourceExecutablePool(DictionarySourceFactory & source_factory);
void registerDictionarySourceHTTP(DictionarySourceFactory & source_factory);

class DictionaryFactory;
void registerDictionaryRangeHashed(DictionaryFactory & factory);
void registerDictionaryComplexKeyHashed(DictionaryFactory & factory);
void registerDictionaryTrie(DictionaryFactory & factory);
void registerDictionaryFlat(DictionaryFactory & factory);
void registerDictionaryHashed(DictionaryFactory & factory);
void registerDictionaryArrayHashed(DictionaryFactory & factory);
void registerDictionaryCache(DictionaryFactory & factory);
void registerDictionaryPolygon(DictionaryFactory & factory);
void registerDictionaryDirect(DictionaryFactory & factory);


void registerDictionaries()
{
    {
        auto & source_factory = DictionarySourceFactory::instance();
        registerDictionarySourceNull(source_factory);
        registerDictionarySourceFile(source_factory);
        registerDictionarySourceMysql(source_factory);
        registerDictionarySourceTimeplus(source_factory);
        registerDictionarySourceClickHouse(source_factory);
        registerDictionarySourcePostgreSQL(source_factory);
        registerDictionarySourceExecutable(source_factory);
        registerDictionarySourceExecutablePool(source_factory);
        registerDictionarySourceHTTP(source_factory);
    }

    {
        auto & factory = DictionaryFactory::instance();
        registerDictionaryRangeHashed(factory);
        registerDictionaryTrie(factory);
        registerDictionaryFlat(factory);
        registerDictionaryHashed(factory);
        registerDictionaryArrayHashed(factory);
        registerDictionaryCache(factory);
        registerDictionaryPolygon(factory);
        registerDictionaryDirect(factory);
    }
}

}
