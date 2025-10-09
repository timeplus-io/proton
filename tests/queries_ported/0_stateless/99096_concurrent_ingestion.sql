-- https://github.com/timeplus-io/proton-enterprise/issues/10067
CREATE RANDOM STREAM gen
(
  `MessageId` string,
  `PropertyId` string,
  `GameId` int64,
  `BetId` int64,
  `BetType` string,
  `ShortBetType` string,
  `GameResult` string,
  `GamingDay` date,
  `Wager` int64,
  `WinLoss` int64,
  `MyTierEarned` int32,
  `ChipSetLabel` string,
  `isBackBet` bool,
  `PP_PlayerId` int64,
  `WynnId` int64,
  `PatronRatingID` int64,
  `TableName` string,
  `SessionId` int64,
  `PitName` string,
  `SeatNumber` string,
  `SupervisorId` string,
  `DealerId` string,
  `StartTime` datetime64(3),
  `EndTime` datetime64(3),
  `TimePlayed` int64,
  `Property` string,
  `OperationTS` datetime64(3),
  `TriggerPredic` string,
  `TheoWinByGameType` float64,
  `TripID` int64,
  `Status` string,
  `TheoWin` decimal(9, 4),
  `CurrentTripPlayTime` int64,
  `region` string,
  `TableMinbet` int64,
  `event_ts` datetime64(3, 'UTC')
)
SETTINGS shards=16;

CREATE STREAM bet_enrich
(
  `MessageId` string,
  `PropertyId` string,
  `GameId` int64,
  `BetId` int64,
  `BetType` low_cardinality(string),
  `ShortBetType` low_cardinality(string),
  `GameResult` string,
  `GamingDay` date,
  `Wager` int64,
  `WinLoss` int64,
  `MyTierEarned` int32,
  `ChipSetLabel` string,
  `isBackBet` bool,
  `PP_PlayerId` int64,
  `WynnId` int64,
  `PatronRatingID` int64,
  `TableName` low_cardinality(string),
  `SessionId` int64,
  `PitName` string,
  `SeatNumber` string,
  `SupervisorId` string,
  `DealerId` string,
  `StartTime` datetime64(3),
  `EndTime` datetime64(3),
  `TimePlayed` int64,
  `Property` low_cardinality(string),
  `OperationTS` datetime64(3),
  `TriggerPredic` string,
  `TheoWinByGameType` float64,
  `TripID` int64,
  `Status` string,
  `TheoWin` nullable(decimal(9, 4)),
  `CurrentTripPlayTime` int64,
  `region` nullable(string),
  `TableMinbet` nullable(int64),
  `event_ts` datetime64(3, 'UTC')
)
ENGINE = Stream(8, weak_hash32(WynnId));

INSERT INTO bet_enrich settings enable_concurrent_ingest=1 SELECT * FROM gen LIMIT 3000 SETTINGS eps=3000;

SELECT sleep(3) FORMAT Null;
SELECT count() FROM table(bet_enrich);

DROP STREAM bet_enrich;
DROP STREAM gen;
