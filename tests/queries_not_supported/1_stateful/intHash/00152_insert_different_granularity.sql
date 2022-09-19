-- Tags: no-tsan, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

DROP STREAM IF EXISTS fixed_granularity_table;

CREATE TABLE fixed_granularity_table (`WatchID` uint64, `JavaEnable` UInt8, `Title` String, `GoodEvent` Int16, `EventTime` DateTime, `EventDate` date, `CounterID` UInt32, `ClientIP` UInt32, `ClientIP6` FixedString(16), `RegionID` UInt32, `UserID` uint64, `CounterClass` int8, `OS` UInt8, `UserAgent` UInt8, `URL` String, `Referer` String, `URLDomain` String, `RefererDomain` String, `Refresh` UInt8, `IsRobot` UInt8, `RefererCategories` Array(UInt16), `URLCategories` Array(UInt16), `URLRegions` Array(UInt32), `RefererRegions` Array(UInt32), `ResolutionWidth` UInt16, `ResolutionHeight` UInt16, `ResolutionDepth` UInt8, `FlashMajor` UInt8, `FlashMinor` UInt8, `FlashMinor2` String, `NetMajor` UInt8, `NetMinor` UInt8, `UserAgentMajor` UInt16, `UserAgentMinor` FixedString(2), `CookieEnable` UInt8, `JavascriptEnable` UInt8, `IsMobile` UInt8, `MobilePhone` UInt8, `MobilePhoneModel` String, `Params` String, `IPNetworkID` UInt32, `TraficSourceID` int8, `SearchEngineID` UInt16, `SearchPhrase` String, `AdvEngineID` UInt8, `IsArtifical` UInt8, `WindowClientWidth` UInt16, `WindowClientHeight` UInt16, `ClientTimeZone` Int16, `ClientEventTime` DateTime, `SilverlightVersion1` UInt8, `SilverlightVersion2` UInt8, `SilverlightVersion3` UInt32, `SilverlightVersion4` UInt16, `PageCharset` String, `CodeVersion` UInt32, `IsLink` UInt8, `IsDownload` UInt8, `IsNotBounce` UInt8, `FUniqID` uint64, `HID` UInt32, `IsOldCounter` UInt8, `IsEvent` UInt8, `IsParameter` UInt8, `DontCountHits` UInt8, `WithHash` UInt8, `HitColor` FixedString(1), `UTCEventTime` DateTime, `Age` UInt8, `Sex` UInt8, `Income` UInt8, `Interests` UInt16, `Robotness` UInt8, `GeneralInterests` Array(UInt16), `RemoteIP` UInt32, `RemoteIP6` FixedString(16), `WindowName` int32, `OpenerName` int32, `HistoryLength` Int16, `BrowserLanguage` FixedString(2), `BrowserCountry` FixedString(2), `SocialNetwork` String, `SocialAction` String, `HTTPError` UInt16, `SendTiming` int32, `DNSTiming` int32, `ConnectTiming` int32, `ResponseStartTiming` int32, `ResponseEndTiming` int32, `FetchTiming` int32, `RedirectTiming` int32, `DOMInteractiveTiming` int32, `DOMContentLoadedTiming` int32, `DOMCompleteTiming` int32, `LoadEventStartTiming` int32, `LoadEventEndTiming` int32, `NSToDOMContentLoadedTiming` int32, `FirstPaintTiming` int32, `RedirectCount` int8, `SocialSourceNetworkID` UInt8, `SocialSourcePage` String, `ParamPrice` Int64, `ParamOrderID` String, `ParamCurrency` FixedString(3), `ParamCurrencyID` UInt16, `GoalsReached` Array(UInt32), `OpenstatServiceName` String, `OpenstatCampaignID` String, `OpenstatAdID` String, `OpenstatSourceID` String, `UTMSource` String, `UTMMedium` String, `UTMCampaign` String, `UTMContent` String, `UTMTerm` String, `FromTag` String, `HasGCLID` UInt8, `RefererHash` uint64, `URLHash` uint64, `CLID` UInt32, `YCLID` uint64, `ShareService` String, `ShareURL` String, `ShareTitle` String, `ParsedParams.Key1` Array(String), `ParsedParams.Key2` Array(String), `ParsedParams.Key3` Array(String), `ParsedParams.Key4` Array(String), `ParsedParams.Key5` Array(String), `ParsedParams.ValueDouble` Array(float64), `IslandID` FixedString(16), `RequestNum` UInt32, `RequestTry` UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192, index_granularity_bytes=0, min_bytes_for_wide_part = 0; -- looks like default table before update

ALTER STREAM fixed_granularity_table REPLACE PARTITION 201403 FROM test.hits;

INSERT INTO fixed_granularity_table SELECT * FROM test.hits LIMIT 10; -- should still have non adaptive granularity

INSERT INTO fixed_granularity_table SELECT * FROM test.hits LIMIT 10;

-- We have removed testing of OPTIMIZE because it's too heavy on very slow builds (debug + coverage + thread fuzzer with sleeps)
-- OPTIMIZE STREAM fixed_granularity_table FINAL; -- and even after optimize

DETACH TABLE fixed_granularity_table;

ATTACH TABLE fixed_granularity_table;

ALTER STREAM fixed_granularity_table DETACH PARTITION 201403;

ALTER STREAM fixed_granularity_table ATTACH PARTITION 201403;

SELECT count() from fixed_granularity_table;

DROP STREAM IF EXISTS fixed_granularity_table;

ALTER STREAM test.hits DETACH PARTITION 201403;

ALTER STREAM test.hits ATTACH PARTITION 201403;

DROP STREAM IF EXISTS hits_copy;

CREATE TABLE hits_copy (`WatchID` uint64, `JavaEnable` UInt8, `Title` String, `GoodEvent` Int16, `EventTime` DateTime, `EventDate` date, `CounterID` UInt32, `ClientIP` UInt32, `ClientIP6` FixedString(16), `RegionID` UInt32, `UserID` uint64, `CounterClass` int8, `OS` UInt8, `UserAgent` UInt8, `URL` String, `Referer` String, `URLDomain` String, `RefererDomain` String, `Refresh` UInt8, `IsRobot` UInt8, `RefererCategories` Array(UInt16), `URLCategories` Array(UInt16), `URLRegions` Array(UInt32), `RefererRegions` Array(UInt32), `ResolutionWidth` UInt16, `ResolutionHeight` UInt16, `ResolutionDepth` UInt8, `FlashMajor` UInt8, `FlashMinor` UInt8, `FlashMinor2` String, `NetMajor` UInt8, `NetMinor` UInt8, `UserAgentMajor` UInt16, `UserAgentMinor` FixedString(2), `CookieEnable` UInt8, `JavascriptEnable` UInt8, `IsMobile` UInt8, `MobilePhone` UInt8, `MobilePhoneModel` String, `Params` String, `IPNetworkID` UInt32, `TraficSourceID` int8, `SearchEngineID` UInt16, `SearchPhrase` String, `AdvEngineID` UInt8, `IsArtifical` UInt8, `WindowClientWidth` UInt16, `WindowClientHeight` UInt16, `ClientTimeZone` Int16, `ClientEventTime` DateTime, `SilverlightVersion1` UInt8, `SilverlightVersion2` UInt8, `SilverlightVersion3` UInt32, `SilverlightVersion4` UInt16, `PageCharset` String, `CodeVersion` UInt32, `IsLink` UInt8, `IsDownload` UInt8, `IsNotBounce` UInt8, `FUniqID` uint64, `HID` UInt32, `IsOldCounter` UInt8, `IsEvent` UInt8, `IsParameter` UInt8, `DontCountHits` UInt8, `WithHash` UInt8, `HitColor` FixedString(1), `UTCEventTime` DateTime, `Age` UInt8, `Sex` UInt8, `Income` UInt8, `Interests` UInt16, `Robotness` UInt8, `GeneralInterests` Array(UInt16), `RemoteIP` UInt32, `RemoteIP6` FixedString(16), `WindowName` int32, `OpenerName` int32, `HistoryLength` Int16, `BrowserLanguage` FixedString(2), `BrowserCountry` FixedString(2), `SocialNetwork` String, `SocialAction` String, `HTTPError` UInt16, `SendTiming` int32, `DNSTiming` int32, `ConnectTiming` int32, `ResponseStartTiming` int32, `ResponseEndTiming` int32, `FetchTiming` int32, `RedirectTiming` int32, `DOMInteractiveTiming` int32, `DOMContentLoadedTiming` int32, `DOMCompleteTiming` int32, `LoadEventStartTiming` int32, `LoadEventEndTiming` int32, `NSToDOMContentLoadedTiming` int32, `FirstPaintTiming` int32, `RedirectCount` int8, `SocialSourceNetworkID` UInt8, `SocialSourcePage` String, `ParamPrice` Int64, `ParamOrderID` String, `ParamCurrency` FixedString(3), `ParamCurrencyID` UInt16, `GoalsReached` Array(UInt32), `OpenstatServiceName` String, `OpenstatCampaignID` String, `OpenstatAdID` String, `OpenstatSourceID` String, `UTMSource` String, `UTMMedium` String, `UTMCampaign` String, `UTMContent` String, `UTMTerm` String, `FromTag` String, `HasGCLID` UInt8, `RefererHash` uint64, `URLHash` uint64, `CLID` UInt32, `YCLID` uint64, `ShareService` String, `ShareURL` String, `ShareTitle` String, `ParsedParams.Key1` Array(String), `ParsedParams.Key2` Array(String), `ParsedParams.Key3` Array(String), `ParsedParams.Key4` Array(String), `ParsedParams.Key5` Array(String), `ParsedParams.ValueDouble` Array(float64), `IslandID` FixedString(16), `RequestNum` UInt32, `RequestTry` UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192, index_granularity_bytes=0, min_bytes_for_wide_part = 0;

ALTER STREAM hits_copy REPLACE PARTITION 201403 FROM test.hits;

-- It's important to test table, which were created before server update
INSERT INTO test.hits SELECT * FROM hits_copy LIMIT 100;

ALTER STREAM test.hits DETACH PARTITION 201403;

ALTER STREAM test.hits ATTACH PARTITION 201403;

-- OPTIMIZE STREAM test.hits;

SELECT count() FROM test.hits;

-- restore hits
ALTER STREAM test.hits REPLACE PARTITION 201403 FROM hits_copy;

DROP STREAM IF EXISTS hits_copy;
