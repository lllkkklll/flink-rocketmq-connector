package kai.lu.rocketmq.flink.legacy.common.config;

/** Config for #{@link StartupMode#GROUP_OFFSETS}. */
public enum OffsetResetStrategy {
    /** If group offsets is not found,the latest offset would be set to start consumer */
    LATEST,
    /** If group offsets is not found,the earliest offset would be set to start consumer */
    EARLIEST
}
