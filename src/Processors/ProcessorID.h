#pragma once

#include <base/types.h>

namespace DB
{
/// Don't change ProcessorID value since they are used to persistent
/// DAG on storage, changing the ID value for a processor is breaking change
enum class ProcessorID : UInt32
{
    InvalidID = 0,
    /// Internal processor
    FilterTransformID = 1,
    ExpressionTransformID = 2,
    ResizeProcessorID = 3,
    StrictResizeProcessorID = 4,
    OffsetTransformID = 5,
    ForkProcessorID = 6,
    LimitTransformID = 7,
    ConcatProcessorID = 8,
    AddingSelectorTransformID = 9,
    AddingDefaultsTransformID = 10,
    ConvertingAggregatedToChunksTransformID = 11,
    QueueBufferID = 12,
    CreatingSetsTransformID = 13,
    CubeTransformID = 14,
    BufferingToFileTransformID = 15,
    MergingAggregatedTransformID = 16,
    RollupTransformID = 17,
    TTLCalcTransformID = 18,
    TTLTransformID = 19,
    DistinctTransformID = 20,
    DistinctSortedTransformID = 21,
    FinalizeAggregatedTransformID = 22,
    ExceptionKeepingTransformID = 23,
    FillingTransformID = 24,
    CopyTransformID = 25,
    ArrayJoinTransformID = 26,
    ExtremesTransformID = 27,
    CheckSortedTransformID = 28,
    TotalsHavingTransformID = 29,
    MergeSortingTransformID = 30,
    PartialSortingTransformID = 31,
    ReverseTransformID = 32,
    LimitByTransformID = 33,
    IntersectOrExceptTransformID = 34,
    JoiningTransformID = 35,
    MaterializingTransformID = 36,
    LimitsCheckingTransformID = 37,
    FinishSortingTransformID = 38,
    FillingRightJoinSideTransformID = 39,
    CopyingDataToViewsTransformID = 40,
    FinalizingViewsTransformID = 41,
    WindowTransformID = 42,
    WatermarkTransformID = 43,
    WindowAssignmentTransformID = 44,
    StreamingJoinTransformID = 45,
    DedupTransformID = 46,
    TimestampTransformID = 47,
    ProcessTimeFilterID = 48,
    CheckMaterializedViewValidTransformID = 49,
    StreamingWindowTransformID = 50,
    WatermarkTransformWithSubstreamID = 51,
    SendingChunkHeaderTransformID = 52,
    AggregatingSortedTransformID = 53,
    CollapsingSortedTransformID = 54,
    FinishAggregatingInOrderTransformID = 55,
    GraphiteRollupSortedTransformID = 56,
    MergingSortedTransformID = 57,
    ReplacingSortedTransformID = 58,
    SummingSortedTransformID = 59,
    VersionedCollapsingTransformID = 60,
    ColumnGathererTransformID = 61,
    TransformWithAdditionalColumnsID = 62,
    ConvertingTransformID = 63,
    ExecutingInnerQueryFromViewTransformID = 64,
    CheckConstraintsTransformID = 65,
    CountingTransformID = 66,
    SquashingChunksTransformID = 67,
    PushingToMaterializedViewMemorySinkID = 68,
    RemoteSinkID = 69,
    BufferSinkID = 70,
    StorageFileSinkID = 71,
    MemorySinkID = 72,
    StorageS3SinkID = 73,
    StorageURLSinkID = 74,
    PartitionedStorageURLSinkID = 75,
    DistributedSinkID = 76,
    MergeTreeSinkID = 77,
    EmbeddedRocksDBSinkID = 78,
    StreamSinkID = 79,
    SetOrJoinSinkID = 80,
    NullSinkToStorageID = 81,
    PartitionedStorageFileSinkID = 82,
    ReplacingWindowColumnTransformID = 83,
    SessionTransformID = 84,
    SessionTransformWithSubstreamID = 85,
    ShufflingTransformID = 86,
    MergeJoinTransformID = 87,

    /// Aggregating transform
    AggregatingInOrderTransformID = 1'000,
    AggregatingTransformID = 1'001,
    GroupingAggregatedTransformID = 1'002,
    MergingAggregatedBucketTransformID = 1'003,
    SortingAggregatedTransformID = 1'004,
    GlobalAggregatingTransformID = 1'005,
    TumbleHopAggregatingTransformID = 1'006,
    SessionAggregatingTransformID = 1'007,
    TumbleHopAggregatingTransformWithSubstreamID = 1'008,
    SessionAggregatingTransformWithSubstreamID = 1'009,
    GlobalAggregatingTransformWithSubstreamID = 1'010,
    UserDefinedEmitStrategyAggregatingTransformID = 1'011,
    UserDefinedEmitStrategyAggregatingTransformWithSubstreamID = 1'012,

    /// Format Input Processors
    ParallelParsingInputFormatID = 4'000,
    AvroConfluentRowInputFormatID = 4'001,
    AvroRowInputFormatID = 4'002,
    CapnProtoRowInputFormatID = 4'003,
    JSONAsRowInputFormatID = 4'004,
    JSONEachRowRowInputFormatID = 4'005,
    LineAsStringRowInputFormatID = 4'006,
    MsgPackRowInputFormatID = 4'007,
    ProtobufRowInputFormatID = 4'008,
    RawBLOBRowInputFormatID = 4'009,
    RawStoreInputFormatID = 4'010,
    RegexpRowInputFormatID = 4'011,
    TSKVRowInputFormatID = 4'012,
    JSONCompactRowOutputFormatID = 4'013,
    JSONColumnsBlockInputFormatBaseID = 4'014,
    JSONColumnsBlockOutputFormatID = 4'015,
    JSONColumnsWithMetadataBlockOutputFormatID = 4'016,
    ODBCDriver2BlockOutputFormatID = 4'017,
    JSONCompactEachRowRowInputFormatID = 4'018,
    CSVRowInputFormatID = 4'019,
    CustomSeparatedRowInputFormatID = 4'020,
    JSONAsObjectRowInputFormatID = 4'021,
    BinaryRowInputFormatID = 4'022,
    TemplateRowInputFormatID = 4'023,
    NativeInputFormatID = 4'024,
    TabSeparatedRowInputFormatID = 4'025,
    ValuesBlockInputFormatID = 4'026,
    ArrowBlockInputFormatID = 4'027,
    ORCBlockInputFormatID = 4'028,
    ParquetBlockInputFormatID = 4'029,

    /// Format Output Processors
    NullOutputFormatID = 5'000,
    AvroRowOutputFormatID = 5'001,
    BinaryRowOutputFormatID = 5'002,
    CapnProtoRowOutputFormatID = 5'003,
    CSVRowOutputFormatID = 5'004,
    CustomSeparatedRowOutputFormatID = 5'005,
    JSONCompactEachRowRowOutputFormatID = 5'006,
    JSONEachRowRowOutputFormatID = 5'007,
    JSONRowOutputFormatID = 5'008,
    MarkdownRowOutputFormatID = 5'009,
    MsgPackRowOutputFormatID = 5'010,
    ProtobufRowOutputFormatID = 5'011,
    RawBLOBRowOutputFormatID = 5'012,
    TabSeparatedRowOutputFormatID = 5'013,
    ValuesRowOutputFormatID = 5'014,
    VerticalRowOutputFormatID = 5'015,
    XMLRowOutputFormatID = 5'016,
    LazyOutputFormatID = 5'017,
    ParallelFormattingOutputFormatID = 5'018,
    PullingOutputFormatID = 5'019,
    JSONEachRowWithProgressRowOutputFormatID = 5'020,
    JSONColumnsBlockOutputFormatBaseID = 5'021,
    PrettyBlockOutputFormatID = 5'022,
    PostgreSQLOutputFormatID = 5'023,
    NativeOutputFormatID = 5'024,
    TSKVRowOutputFormatID = 5'025,
    TemplateBlockOutputFormatID = 5'026,
    ArrowBlockOutputFormatID = 5'027,
    ORCBlockOutputFormatID = 5'028,
    ParquetBlockOutputFormatID = 5'029,

    /// Source Processors
    NullSourceID = 10'000,
    KafkaSourceID = 10'001,
    EmbeddedRocksDBSourceID = 10'002,
    FileLogSourceID = 10'003,
    BlockListSourceID = 10'004,
    MergeSorterSourceID = 10'005,
    SyncKillQuerySourceID = 10'006,
    WaitForAsyncInsertSourceID = 10'007,
    JoinSourceID = 10'008,
    GenerateSourceID = 10'009,
    StorageInputSourceID = 10'010,
    StorageFileSourceID = 10'011,
    BufferSourceID = 10'012,
    MemorySourceID = 10'013,
    StorageURLSourceID = 10'014,
    DirectoryMonitorSourceID = 10'015,
    MergeTreeSequentialSourceID = 10'016,
    MergeTreeThreadSelectProcessorID = 10'017,
    MergeTreeSelectProcessorID = 10'018,
    DelayedPortsProcessorID = 10'019,
    PushingSourceID = 10'020,
    PushingAsyncSourceID = 10'021,
    SourceFromNativeStreamID = 10'022,
    ConvertingAggregatedToChunksSourceID = 10'023,
    StreamingSourceFromNativeStreamID = 10'024,
    StreamingConvertingAggregatedToChunksSourceID = 10'025,
    StreamingConvertingAggregatedToChunksTransformID = 10'026,
    StreamingStoreSourceID = 10'027,
    ShellCommandSourceID = 10'028,
    TemporaryFileLazySourceID = 10'029,
    SourceFromSingleChunkID = 10'030,
    DelayedSourceID = 10'031,
    StreamingStoreSourceChannelID = 10'032,
    RemoteSourceID = 10'033,
    RemoteTotalsSourceID = 10'034,
    RemoteExtremesSourceID = 10'035,
    DictionarySourceID = 10'036,
    MarkSourceID = 10'037,
    ColumnsSourceID = 10'038,
    DataSkippingIndicesSourceID = 10'039,
    NumbersMultiThreadedSourceID = 10'040,
    NumbersSourceID = 10'041,
    TablesBlockSourceID = 10'042,
    ZerosSourceID = 10'043,
    StorageS3SourceID = 10'044,
    GenerateRandomSourceID = 10'045,
    SourceFromQueryPipelineID = 10'046,

    /// Sink Processors
    EmptySinkID = 20'000,
    NullSinkID = 20'001,
    ExternalTableDataSinkID = 20'002,
};

inline ProcessorID toProcessID(UInt32 v)
{
    auto pid = ProcessorID::InvalidID;
    switch (v)
    {
        case 1:
            pid = ProcessorID::FillingTransformID;
            break;
        case 2:
            pid = ProcessorID::ExpressionTransformID;
            break;
        case 3:
            pid = ProcessorID::ResizeProcessorID;
            break;
        case 4:
            pid = ProcessorID::StrictResizeProcessorID;
            break;
        case 5:
            pid = ProcessorID::OffsetTransformID;
            break;
        case 6:
            pid = ProcessorID::ForkProcessorID;
            break;
        case 7:
            pid = ProcessorID::LimitTransformID;
            break;
        case 8:
            pid = ProcessorID::ConcatProcessorID;
            break;
        case 9:
            pid = ProcessorID::AddingSelectorTransformID;
            break;
        case 10:
            pid = ProcessorID::AddingDefaultsTransformID;
            break;
        case 11:
            pid = ProcessorID::ConvertingAggregatedToChunksTransformID;
            break;
        case 12:
            pid = ProcessorID::QueueBufferID;
            break;
        case 13:
            pid = ProcessorID::CreatingSetsTransformID;
            break;
        case 14:
            pid = ProcessorID::CubeTransformID;
            break;
        case 15:
            pid = ProcessorID::BufferingToFileTransformID;
            break;
        case 16:
            pid = ProcessorID::MergingAggregatedTransformID;
            break;
        case 17:
            pid = ProcessorID::RollupTransformID;
            break;
        case 18:
            pid = ProcessorID::TTLCalcTransformID;
            break;
        case 19:
            pid = ProcessorID::TTLTransformID;
            break;
        case 20:
            pid = ProcessorID::DistinctTransformID;
            break;
        case 21:
            pid = ProcessorID::DistinctSortedTransformID;
            break;
        case 22:
            pid = ProcessorID::FinalizeAggregatedTransformID;
            break;
        case 23:
            pid = ProcessorID::ExceptionKeepingTransformID;
            break;
        case 24:
            pid = ProcessorID::FillingTransformID;
            break;
        case 25:
            pid = ProcessorID::CopyTransformID;
            break;
        case 26:
            pid = ProcessorID::ArrayJoinTransformID;
            break;
        case 27:
            pid = ProcessorID::ExtremesTransformID;
            break;
        case 28:
            pid = ProcessorID::CheckSortedTransformID;
            break;
        case 29:
            pid = ProcessorID::TotalsHavingTransformID;
            break;
        case 30:
            pid = ProcessorID::MergeSortingTransformID;
            break;
        case 31:
            pid = ProcessorID::PartialSortingTransformID;
            break;
        case 32:
            pid = ProcessorID::ReverseTransformID;
            break;
        case 33:
            pid = ProcessorID::LimitByTransformID;
            break;
        case 34:
            pid = ProcessorID::IntersectOrExceptTransformID;
            break;
        case 35:
            pid = ProcessorID::JoiningTransformID;
            break;
        case 36:
            pid = ProcessorID::MaterializingTransformID;
            break;
        case 37:
            pid = ProcessorID::LimitsCheckingTransformID;
            break;
        case 38:
            pid = ProcessorID::FinishSortingTransformID;
            break;
        case 39:
            pid = ProcessorID::FillingRightJoinSideTransformID;
            break;
        case 40:
            pid = ProcessorID::CopyingDataToViewsTransformID;
            break;
        case 41:
            pid = ProcessorID::FinalizingViewsTransformID;
            break;
        case 42:
            pid = ProcessorID::WindowTransformID;
            break;
        case 43:
            pid = ProcessorID::WatermarkTransformID;
            break;
        case 44:
            pid = ProcessorID::WindowAssignmentTransformID;
            break;
        case 45:
            pid = ProcessorID::StreamingJoinTransformID;
            break;
        case 46:
            pid = ProcessorID::DedupTransformID;
            break;
        case 47:
            pid = ProcessorID::TimestampTransformID;
            break;
        case 48:
            pid = ProcessorID::ProcessTimeFilterID;
            break;
        case 49:
            pid = ProcessorID::CheckMaterializedViewValidTransformID;
            break;
        case 50:
            pid = ProcessorID::StreamingWindowTransformID;
            break;
        case 51:
            pid = ProcessorID::WatermarkTransformWithSubstreamID;
            break;
        case 52:
            pid = ProcessorID::SendingChunkHeaderTransformID;
            break;
        case 53:
            pid = ProcessorID::AggregatingSortedTransformID;
            break;
        case 54:
            pid = ProcessorID::CollapsingSortedTransformID;
            break;
        case 55:
            pid = ProcessorID::FinishAggregatingInOrderTransformID;
            break;
        case 56:
            pid = ProcessorID::GraphiteRollupSortedTransformID;
            break;
        case 57:
            pid = ProcessorID::MergingSortedTransformID;
            break;
        case 58:
            pid = ProcessorID::ReplacingSortedTransformID;
            break;
        case 59:
            pid = ProcessorID::SummingSortedTransformID;
            break;
        case 60:
            pid = ProcessorID::VersionedCollapsingTransformID;
            break;
        case 61:
            pid = ProcessorID::ColumnGathererTransformID;
            break;
        case 62:
            pid = ProcessorID::TransformWithAdditionalColumnsID;
            break;
        case 63:
            pid = ProcessorID::ConvertingTransformID;
            break;
        case 64:
            pid = ProcessorID::ExecutingInnerQueryFromViewTransformID;
            break;
        case 65:
            pid = ProcessorID::CheckConstraintsTransformID;
            break;
        case 66:
            pid = ProcessorID::CountingTransformID;
            break;
        case 67:
            pid = ProcessorID::SquashingChunksTransformID;
            break;
        case 68:
            pid = ProcessorID::PushingToMaterializedViewMemorySinkID;
            break;
        case 69:
            pid = ProcessorID::RemoteSinkID;
            break;
        case 70:
            pid = ProcessorID::BufferSinkID;
            break;
        case 71:
            pid = ProcessorID::StorageFileSinkID;
            break;
        case 72:
            pid = ProcessorID::MemorySinkID;
            break;
        case 73:
            pid = ProcessorID::StorageS3SinkID;
            break;
        case 74:
            pid = ProcessorID::StorageURLSinkID;
            break;
        case 75:
            pid = ProcessorID::PartitionedStorageURLSinkID;
            break;
        case 76:
            pid = ProcessorID::DistributedSinkID;
            break;
        case 77:
            pid = ProcessorID::MergeTreeSinkID;
            break;
        case 78:
            pid = ProcessorID::EmbeddedRocksDBSinkID;
            break;
        case 79:
            pid = ProcessorID::StreamSinkID;
            break;
        case 80:
            pid = ProcessorID::SetOrJoinSinkID;
            break;
        case 81:
            pid = ProcessorID::NullSinkToStorageID;
            break;
        case 82:
            pid = ProcessorID::PartitionedStorageFileSinkID;
            break;
        case 83:
            pid = ProcessorID::ReplacingWindowColumnTransformID;
            break;
        case 84:
            pid = ProcessorID::SessionTransformID;
            break;
        case 85:
            pid = ProcessorID::SessionTransformWithSubstreamID;
            break;
        case 86:
            pid = ProcessorID::ShufflingTransformID;
            break;

        /// Aggregating transform
        case 1'000:
            pid = ProcessorID::AggregatingInOrderTransformID;
            break;
        case 1'001:
            pid = ProcessorID::AggregatingTransformID;
            break;
        case 1'002:
            pid = ProcessorID::GroupingAggregatedTransformID;
            break;
        case 1'003:
            pid = ProcessorID::MergingAggregatedBucketTransformID;
            break;
        case 1'004:
            pid = ProcessorID::SortingAggregatedTransformID;
            break;
        case 1'005:
            pid = ProcessorID::GlobalAggregatingTransformID;
            break;
        case 1'006:
            pid = ProcessorID::TumbleHopAggregatingTransformID;
            break;
        case 1'007:
            pid = ProcessorID::SessionAggregatingTransformID;
            break;
        case 1'008:
            pid = ProcessorID::TumbleHopAggregatingTransformWithSubstreamID;
            break;
        case 1'009:
            pid = ProcessorID::SessionAggregatingTransformWithSubstreamID;
            break;
        case 1'010:
            pid = ProcessorID::GlobalAggregatingTransformWithSubstreamID;
            break;
        case 1'011:
            pid = ProcessorID::UserDefinedEmitStrategyAggregatingTransformID;
            break;
        case 1'012:
            pid = ProcessorID::UserDefinedEmitStrategyAggregatingTransformWithSubstreamID;
            break;

        /// Format Input Processors
        case 4'000:
            pid = ProcessorID::ParallelParsingInputFormatID;
            break;
        case 4'001:
            pid = ProcessorID::AvroConfluentRowInputFormatID;
            break;
        case 4'002:
            pid = ProcessorID::AvroRowInputFormatID;
            break;
        case 4'003:
            pid = ProcessorID::CapnProtoRowInputFormatID;
            break;
        case 4'004:
            pid = ProcessorID::JSONAsRowInputFormatID;
            break;
        case 4'005:
            pid = ProcessorID::JSONEachRowRowInputFormatID;
            break;
        case 4'006:
            pid = ProcessorID::LineAsStringRowInputFormatID;
            break;
        case 4'007:
            pid = ProcessorID::MsgPackRowInputFormatID;
            break;
        case 4'008:
            pid = ProcessorID::ProtobufRowInputFormatID;
            break;
        case 4'009:
            pid = ProcessorID::RawBLOBRowInputFormatID;
            break;
        case 4'010:
            pid = ProcessorID::RawStoreInputFormatID;
            break;
        case 4'011:
            pid = ProcessorID::RegexpRowInputFormatID;
            break;
        case 4'012:
            pid = ProcessorID::TSKVRowInputFormatID;
            break;
        case 4'013:
            pid = ProcessorID::JSONCompactRowOutputFormatID;
            break;
        case 4'014:
            pid = ProcessorID::JSONColumnsBlockInputFormatBaseID;
            break;
        case 4'015:
            pid = ProcessorID::JSONColumnsBlockOutputFormatID;
            break;
        case 4'016:
            pid = ProcessorID::JSONColumnsWithMetadataBlockOutputFormatID;
            break;
        case 4'017:
            pid = ProcessorID::ODBCDriver2BlockOutputFormatID;
            break;
        case 4'018:
            pid = ProcessorID::JSONCompactEachRowRowInputFormatID;
            break;
        case 4'019:
            pid = ProcessorID::CSVRowInputFormatID;
            break;
        case 4'020:
            pid = ProcessorID::CustomSeparatedRowInputFormatID;
            break;
        case 4'021:
            pid = ProcessorID::JSONAsObjectRowInputFormatID;
            break;
        case 4'022:
            pid = ProcessorID::BinaryRowInputFormatID;
            break;
        case 4'023:
            pid = ProcessorID::TemplateRowInputFormatID;
            break;
        case 4'024:
            pid = ProcessorID::NativeInputFormatID;
            break;
        case 4'025:
            pid = ProcessorID::TabSeparatedRowInputFormatID;
            break;
        case 4'026:
            pid = ProcessorID::ValuesBlockInputFormatID;
            break;

        /// Format Output Processors
        case 5'000:
            pid = ProcessorID::NullOutputFormatID;
            break;
        case 5'001:
            pid = ProcessorID::AvroRowOutputFormatID;
            break;
        case 5'002:
            pid = ProcessorID::BinaryRowOutputFormatID;
            break;
        case 5'003:
            pid = ProcessorID::CapnProtoRowOutputFormatID;
            break;
        case 5'004:
            pid = ProcessorID::CSVRowOutputFormatID;
            break;
        case 5'005:
            pid = ProcessorID::CustomSeparatedRowOutputFormatID;
            break;
        case 5'006:
            pid = ProcessorID::JSONCompactEachRowRowOutputFormatID;
            break;
        case 5'007:
            pid = ProcessorID::JSONEachRowRowOutputFormatID;
            break;
        case 5'008:
            pid = ProcessorID::JSONRowOutputFormatID;
            break;
        case 5'009:
            pid = ProcessorID::MarkdownRowOutputFormatID;
            break;
        case 5'010:
            pid = ProcessorID::MsgPackRowOutputFormatID;
            break;
        case 5'011:
            pid = ProcessorID::ProtobufRowOutputFormatID;
            break;
        case 5'012:
            pid = ProcessorID::RawBLOBRowOutputFormatID;
            break;
        case 5'013:
            pid = ProcessorID::TabSeparatedRowOutputFormatID;
            break;
        case 5'014:
            pid = ProcessorID::ValuesRowOutputFormatID;
            break;
        case 5'015:
            pid = ProcessorID::VerticalRowOutputFormatID;
            break;
        case 5'016:
            pid = ProcessorID::XMLRowOutputFormatID;
            break;
        case 5'017:
            pid = ProcessorID::LazyOutputFormatID;
            break;
        case 5'018:
            pid = ProcessorID::ParallelFormattingOutputFormatID;
            break;
        case 5'019:
            pid = ProcessorID::PullingOutputFormatID;
            break;
        case 5'020:
            pid = ProcessorID::JSONEachRowWithProgressRowOutputFormatID;
            break;
        case 5'021:
            pid = ProcessorID::JSONColumnsBlockOutputFormatBaseID;
            break;
        case 5'022:
            pid = ProcessorID::PrettyBlockOutputFormatID;
            break;
        case 5'023:
            pid = ProcessorID::PostgreSQLOutputFormatID;
            break;
        case 5'024:
            pid = ProcessorID::NativeOutputFormatID;
            break;
        case 5'025:
            pid = ProcessorID::TSKVRowOutputFormatID;
            break;
        case 5'026:
            pid = ProcessorID::TemplateBlockOutputFormatID;
            break;

        /// Source Processors
        case 10'000:
            pid = ProcessorID::NullSourceID;
            break;
        case 10'001:
            pid = ProcessorID::KafkaSourceID;
            break;
        case 10'002:
            pid = ProcessorID::EmbeddedRocksDBSourceID;
            break;
        case 10'003:
            pid = ProcessorID::FileLogSourceID;
            break;
        case 10'004:
            pid = ProcessorID::BlockListSourceID;
            break;
        case 10'005:
            pid = ProcessorID::MergeSorterSourceID;
            break;
        case 10'006:
            pid = ProcessorID::SyncKillQuerySourceID;
            break;
        case 10'007:
            pid = ProcessorID::WaitForAsyncInsertSourceID;
            break;
        case 10'008:
            pid = ProcessorID::JoinSourceID;
            break;
        case 10'009:
            pid = ProcessorID::GenerateSourceID;
            break;
        case 10'010:
            pid = ProcessorID::StorageInputSourceID;
            break;
        case 10'011:
            pid = ProcessorID::StorageFileSourceID;
            break;
        case 10'012:
            pid = ProcessorID::BufferSourceID;
            break;
        case 10'013:
            pid = ProcessorID::MemorySourceID;
            break;
        case 10'014:
            pid = ProcessorID::StorageURLSourceID;
            break;
        case 10'015:
            pid = ProcessorID::DirectoryMonitorSourceID;
            break;
        case 10'016:
            pid = ProcessorID::MergeTreeSequentialSourceID;
            break;
        case 10'017:
            pid = ProcessorID::MergeTreeThreadSelectProcessorID;
            break;
        case 10'018:
            pid = ProcessorID::MergeTreeSelectProcessorID;
            break;
        case 10'019:
            pid = ProcessorID::DelayedPortsProcessorID;
            break;
        case 10'020:
            pid = ProcessorID::PushingSourceID;
            break;
        case 10'021:
            pid = ProcessorID::PushingAsyncSourceID;
            break;
        case 10'022:
            pid = ProcessorID::SourceFromNativeStreamID;
            break;
        case 10'023:
            pid = ProcessorID::ConvertingAggregatedToChunksSourceID;
            break;
        case 10'024:
            pid = ProcessorID::StreamingSourceFromNativeStreamID;
            break;
        case 10'025:
            pid = ProcessorID::StreamingConvertingAggregatedToChunksSourceID;
            break;
        case 10'026:
            pid = ProcessorID::StreamingConvertingAggregatedToChunksTransformID;
            break;
        case 10'027:
            pid = ProcessorID::StreamingStoreSourceID;
            break;
        case 10'028:
            pid = ProcessorID::ShellCommandSourceID;
            break;
        case 10'029:
            pid = ProcessorID::TemporaryFileLazySourceID;
            break;
        case 10'030:
            pid = ProcessorID::SourceFromSingleChunkID;
            break;
        case 10'031:
            pid = ProcessorID::DelayedSourceID;
            break;
        case 10'032:
            pid = ProcessorID::StreamingStoreSourceChannelID;
            break;
        case 10'033:
            pid = ProcessorID::RemoteSourceID;
            break;
        case 10'034:
            pid = ProcessorID::RemoteTotalsSourceID;
            break;
        case 10'035:
            pid = ProcessorID::RemoteExtremesSourceID;
            break;
        case 10'036:
            pid = ProcessorID::DictionarySourceID;
            break;
        case 10'037:
            pid = ProcessorID::MarkSourceID;
            break;
        case 10'038:
            pid = ProcessorID::ColumnsSourceID;
            break;
        case 10'039:
            pid = ProcessorID::DataSkippingIndicesSourceID;
            break;
        case 10'040:
            pid = ProcessorID::NumbersMultiThreadedSourceID;
            break;
        case 10'041:
            pid = ProcessorID::NumbersSourceID;
            break;
        case 10'042:
            pid = ProcessorID::TablesBlockSourceID;
            break;
        case 10'043:
            pid = ProcessorID::ZerosSourceID;
            break;
        case 10'044:
            pid = ProcessorID::StorageS3SourceID;
            break;
        case 10'045:
            pid = ProcessorID::GenerateRandomSourceID;
            break;
        case 10'046:
            pid = ProcessorID::SourceFromQueryPipelineID;
            break;

            /// Sink Processors
        case 20'000:
            pid = ProcessorID::EmptySinkID;
            break;
        case 20'001:
            pid = ProcessorID::NullSinkID;
            break;
        case 20'002:
            pid = ProcessorID::ExternalTableDataSinkID;
            break;
        default:
            pid = ProcessorID::InvalidID;
            break;
    }

    assert(v == static_cast<UInt32>(pid));
    return pid;
}
}
