#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ExtractFirstSignificantSubdomain.h"


namespace DB
{

template <bool without_www, bool conform_rfc>
struct CutToFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos tmp_data;
        size_t tmp_length;
        Pos domain_end;
        ExtractFirstSignificantSubdomain<without_www, conform_rfc>::execute(data, size, tmp_data, tmp_length, &domain_end);

        if (tmp_length == 0)
            return;

        res_data = tmp_data;
        res_size = domain_end - tmp_data;
    }
};

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cut_to_first_significant_subdomain"; };
using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, false>>, NameCutToFirstSignificantSubdomain>;

struct NameCutToFirstSignificantSubdomainWithWWW { static constexpr auto name = "cut_to_first_significant_subdomain_with_www"; };
using FunctionCutToFirstSignificantSubdomainWithWWW = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, false>>, NameCutToFirstSignificantSubdomainWithWWW>;

struct NameCutToFirstSignificantSubdomainRFC { static constexpr auto name = "cut_to_first_significant_subdomain_rfc"; };
using FunctionCutToFirstSignificantSubdomainRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<true, true>>, NameCutToFirstSignificantSubdomainRFC>;

struct NameCutToFirstSignificantSubdomainWithWWWRFC { static constexpr auto name = "cut_to_first_significant_subdomain_with_www_rfc"; };
using FunctionCutToFirstSignificantSubdomainWithWWWRFC = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain<false, true>>, NameCutToFirstSignificantSubdomainWithWWWRFC>;

REGISTER_FUNCTION(CutToFirstSignificantSubdomain)
{
    factory.registerFunction<FunctionCutToFirstSignificantSubdomain>(
        {
        R"(Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain" (see documentation of the `firstSignificantSubdomain`).)",
        Documentation::Examples{
            {"cutToFirstSignificantSubdomain1", "SELECT cut_to_first_significant_subdomain('https://news.clickhouse.com.tr/')"},
            {"cutToFirstSignificantSubdomain2", "SELECT cut_to_first_significant_subdomain('www.tr')"},
            {"cutToFirstSignificantSubdomain3", "SELECT cut_to_first_significant_subdomain('tr')"},
        },
        Documentation::Categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWW>(
        {
            R"(Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain", without stripping "www".)",
            Documentation::Examples{},
            Documentation::Categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainRFC>(
        {
            R"(Similar to `cut_to_first_significant_subdomain` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
            Documentation::Examples{},
            Documentation::Categories{"URL"}
        });
    factory.registerFunction<FunctionCutToFirstSignificantSubdomainWithWWWRFC>(
        {
            R"(Similar to `cut_to_first_significant_subdomain_with_www` but follows stricter rules to be compatible with RFC 3986 and less performant.)",
            Documentation::Examples{},
            Documentation::Categories{"URL"}
        });
}

}
