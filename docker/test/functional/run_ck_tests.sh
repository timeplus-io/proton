#!/bin/bash

/entrypoint.sh --daemon

# waiting for proton server startup
sleep 10s

visits_tsv="/datasets/visits_v1.tsv"
hits_tsv="/datasets/hits_v1.tsv"

if [ ! -f  "${visits_tsv}" ] || [ ! -f "${hits_tsv}" ]; then
    echo "Didn't find visits_v1.tsv or hits_v1.tsv in data directory: ${2}"
    usage
fi

# Create test database and streams
echo "Provisioning test database"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="create database if not exists test"

echo "Provisioning test.hits"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="drop stream if exists test.hits"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="create stream if not exists test.hits ( WatchID uint64, JavaEnable uint8, Title string, GoodEvent int16, EventTime datetime, EventDate date, CounterID uint32, ClientIP uint32, ClientIP6 fixed_string(16), RegionID uint32, UserID uint64, CounterClass int8, OS uint8, UserAgent uint8, URL string, Referer string, URLDomain string, RefererDomain string, Refresh uint8, IsRobot uint8, RefererCategories array(uint16), URLCategories array(uint16), URLRegions array(uint32), RefererRegions array(uint32), ResolutionWidth uint16, ResolutionHeight uint16, ResolutionDepth uint8, FlashMajor uint8, FlashMinor uint8, FlashMinor2 string, NetMajor uint8, NetMinor uint8, UserAgentMajor uint16, UserAgentMinor fixed_string(2), CookieEnable uint8, JavascriptEnable uint8, IsMobile uint8, MobilePhone uint8, MobilePhoneModel string, Params string, IPNetworkID uint32, TraficSourceID int8, SearchEngineID uint16, SearchPhrase string, AdvEngineID uint8, IsArtifical uint8, WindowClientWidth uint16, WindowClientHeight uint16, ClientTimeZone int16, ClientEventTime datetime, SilverlightVersion1 uint8, SilverlightVersion2 uint8, SilverlightVersion3 uint32, SilverlightVersion4 uint16, PageCharset string, CodeVersion uint32, IsLink uint8, IsDownload uint8, IsNotBounce uint8, FUniqID uint64, HID uint32, IsOldCounter uint8, IsEvent uint8, IsParameter uint8, DontCountHits uint8, WithHash uint8, HitColor fixed_string(1), UTCEventTime datetime, Age uint8, Sex uint8, Income uint8, interests uint16, Robotness uint8, GeneralInterests array(uint16), RemoteIP uint32, RemoteIP6 fixed_string(16), WindowName int32, OpenerName int32, HistoryLength int16, BrowserLanguage fixed_string(2), BrowserCountry fixed_string(2), SocialNetwork string, SocialAction string, HTTPError uint16, SendTiming int32, DNSTiming int32, ConnectTiming int32, ResponseStartTiming int32, ResponseEndTiming int32, FetchTiming int32, RedirectTiming int32, DOMinteractiveTiming int32, DOMContentLoadedTiming int32, DOMCompleteTiming int32, LoadEventStartTiming int32, LoadEventEndTiming int32, NSToDOMContentLoadedTiming int32, FirstPaintTiming int32, RedirectCount int8, SocialSourceNetworkID uint8, SocialSourcePage string, ParamPrice int64, ParamOrderID string, ParamCurrency fixed_string(3), ParamCurrencyID uint16, GoalsReached array(uint32), OpenstatServiceName string, OpenstatCampaignID string, OpenstatAdID string, OpenstatSourceID string, UTMSource string, UTMMedium string, UTMCampaign string, UTMContent string, UTMTerm string, FromTag string, HasGCLID uint8, RefererHash uint64, URLHash uint64, CLID uint32, YCLID uint64, ShareService string, ShareURL string, ShareTitle string, ParsedParams nested(Key1 string, Key2 string, Key3 string, Key4 string, Key5 string, ValueDouble float64), IslandID fixed_string(16), RequestNum uint32, RequestTry uint8 ) ENGINE = MergeTree() PARTITION BY to_YYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity = 8192"


echo "Provisioning test.visits"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="drop stream if exists test.visits"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="create stream if not exists test.visits ( CounterID uint32, StartDate Date, Sign int8, IsNew uint8, VisitID uint64, UserID uint64, StartTime datetime, Duration uint32, UTCStartTime datetime, PageViews int32, Hits int32, IsBounce uint8, Referer string, StartURL string, RefererDomain string, StartURLDomain string, EndURL string, LinkURL string, IsDownload uint8, TraficSourceID int8, SearchEngineID uint16, SearchPhrase string, AdvEngineID uint8, PlaceID int32, RefererCategories array(uint16), URLCategories array(uint16), URLRegions array(uint32), RefererRegions array(uint32), IsYandex uint8, GoalReachesDepth int32, GoalReachesURL int32, GoalReachesAny int32, SocialSourceNetworkID uint8, SocialSourcePage string, MobilePhoneModel string, ClientEventTime datetime, RegionID uint32, ClientIP uint32, ClientIP6 fixed_string(16), RemoteIP uint32, RemoteIP6 fixed_string(16), IPNetworkID uint32, SilverlightVersion3 uint32, CodeVersion uint32, ResolutionWidth uint16, ResolutionHeight uint16, UserAgentMajor uint16, UserAgentMinor uint16, WindowClientWidth uint16, WindowClientHeight uint16, SilverlightVersion2 uint8, SilverlightVersion4 uint16, FlashVersion3 uint16, FlashVersion4 uint16, ClientTimeZone int16, OS uint8, UserAgent uint8, ResolutionDepth uint8, FlashMajor uint8, FlashMinor uint8, NetMajor uint8, NetMinor uint8, MobilePhone uint8, SilverlightVersion1 uint8, Age uint8, Sex uint8, Income uint8, JavaEnable uint8, CookieEnable uint8, JavascriptEnable uint8, IsMobile uint8, BrowserLanguage uint16, BrowserCountry uint16, interests uint16, Robotness uint8, GeneralInterests array(uint16), Params array(string), Goals nested(ID uint32, Serial uint32, EventTime datetime, Price int64, OrderID string, CurrencyID uint32), WatchIDs array(uint64), ParamSumPrice int64, ParamCurrency fixed_string(3), ParamCurrencyID uint16, ClickLogID uint64, ClickEventID int32, ClickGoodEvent int32, ClickEventTime datetime, ClickPriorityID int32, ClickPhraseID int32, ClickPageID int32, ClickPlaceID int32, ClickTypeID int32, ClickResourceID int32, ClickCost uint32, ClickClientIP uint32, ClickDomainID uint32, ClickURL string, ClickAttempt uint8, ClickOrderID uint32, ClickBannerID uint32, ClickMarketCategoryID uint32, ClickMarketPP uint32, ClickMarketCategoryName string, ClickMarketPPName string, ClickAWAPSCampaignName string, ClickPageName string, ClickTargetType uint16, ClickTargetPhraseID uint64, ClickContextType uint8, ClickSelectType int8, ClickOptions string, ClickGroupBannerID int32, OpenstatServiceName string, OpenstatCampaignID string, OpenstatAdID string, OpenstatSourceID string, UTMSource string, UTMMedium string, UTMCampaign string, UTMContent string, UTMTerm string, FromTag string, HasGCLID uint8, FirstVisit datetime, PredLastVisit Date, LastVisit Date, TotalVisits uint32, TraficSource nested(ID int8, SearchEngineID uint16, AdvEngineID uint8, PlaceID uint16, SocialSourceNetworkID uint8, Domain string, SearchPhrase string, SocialSourcePage string), Attendance fixed_string(16), CLID uint32, YCLID uint64, NormalizedRefererHash uint64, SearchPhraseHash uint64, RefererDomainHash uint64, NormalizedStartURLHash uint64, StartURLDomainHash uint64, NormalizedEndURLHash uint64, TopLevelDomain uint64, URLScheme uint64, OpenstatServiceNameHash uint64, OpenstatCampaignIDHash uint64, OpenstatAdIDHash uint64, OpenstatSourceIDHash uint64, UTMSourceHash uint64, UTMMediumHash uint64, UTMCampaignHash uint64, UTMContentHash uint64, UTMTermHash uint64, FromHash uint64, WebVisorEnabled uint8, WebVisorActivity uint32, ParsedParams nested(Key1 string, Key2 string, Key3 string, Key4 string, Key5 string, ValueDouble float64), Market nested(Type uint8, GoalID uint32, OrderID string, OrderPrice int64, PP uint32, DirectPlaceID uint32, DirectOrderID uint32, DirectBannerID uint32, GoodID string, GoodName string, GoodQuantity int32, GoodPrice int64), IslandID fixed_string(16) ) ENGINE = MergeTree() PARTITION BY to_YYYYMM(StartDate) ORDER BY (CounterID, StartDate, VisitID) SETTINGS index_granularity = 8192"

# Load the data
echo "Loading data ${visits_tsv}"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="insert into test.visits format TSV" < "${visits_tsv}"

echo "Loading data ${hits_tsv}"
proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="insert into test.hits format TSV" < "${hits_tsv}"

echo "Run test cases"
$PROTON_SRC/tests/ported-clickhouse-test.py $TEST_TYPE --jobs 4 -q $PROTON_SRC/tests/queries_ported 2>&1 \
    | ts '%Y-%m-%d %H:%M:%S' \
    | tee -a /test_output/test_result.txt

/process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

# Compress tables.
#
# NOTE:
# - that due to tests with s3 storage we cannot use /var/lib/clickhouse/data
#   directly
# - even though ci auto-compress some files (but not *.tsv) it does this only
#   for files >64MB, we want this files to be compressed explicitly
for table in query_log trace_log
do
    proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="select * from system.$table format TSVWithNamesAndTypes" | pigz > /test_output/$table.tsv.gz ||:
done

# Also export trace log in flamegraph-friendly format.
# FIXME: count(*) is work for CPU and Real,
# but for Memory/MemorySample/MemoryPeak, we should use `size` column.
for trace_type in CPU Memory Real
do
    proton-client -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --query="
            select
                array_string_concat((array_map(x -> concat(split_by_char('/', address_to_line(x))[-1], '#', demangle(address_to_symbol(x)) ), trace)), ';') AS stack,
                count(*) AS samples
            from system.trace_log
            where trace_type = '$trace_type'
            group by trace
            order by samples desc
            settings allow_introspection_functions = 1
            format TabSeparated" \
        > "/test_output/trace-log-$trace_type-flamegraph.tsv" ||:
    /flamegraph.pl "/test_output/trace-log-$trace_type-flamegraph.tsv" > "/test_output/trace-log-$trace_type-flamegraph.svg" ||:
done

grep -Fa "Fatal" /var/log/proton-server/proton-server.log ||:

mkdir /test_output/log
cp /var/log/proton-server/proton-server.log /test_output/log/proton-server.log ||:
cp /var/log/proton-server/proton-server.err.log /test_output/log/proton-server.err.log ||:
