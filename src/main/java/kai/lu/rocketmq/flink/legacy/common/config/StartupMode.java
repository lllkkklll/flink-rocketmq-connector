package kai.lu.rocketmq.flink.legacy.common.config;

/** RocketMQ startup mode. */
public enum StartupMode {

    EARLIEST,

    LATEST,

    GROUP_OFFSETS,

    TIMESTAMP,

    SPECIFIC_OFFSETS
}
