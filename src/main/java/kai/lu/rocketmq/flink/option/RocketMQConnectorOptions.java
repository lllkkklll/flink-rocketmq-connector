package kai.lu.rocketmq.flink.option;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.List;

import static kai.lu.rocketmq.flink.option.RocketMQConstant.*;
import static org.apache.flink.configuration.description.TextElement.text;

public class RocketMQConnectorOptions {

    // --------------------------------------------------------------------------------------------
    // RocketMQ specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> OPTION_PROPERTIES_NAMESERVER_ADDRESS = ConfigOptions
            .key(PROPERTIES_NAMESERVER_ADDRESS)
            .stringType()
            .noDefaultValue()
            .withDescription("Nameserver address Required.");

    public static final ConfigOption<String> OPTION_PROPERTIES_TOPIC = ConfigOptions
            .key(PROPERTIES_TOPIC)
            .stringType()
            .noDefaultValue()
            .withDescription("Topic name of the RocketMQ record Required.");

    public static final ConfigOption<String> OPTION_PROPERTIES_GROUP_ID = ConfigOptions
            .key(PROPERTIES_GROUP_ID)
            .stringType()
            .noDefaultValue()
            .withDescription("Consumer and Product group.");

    public static final ConfigOption<String> OPTION_TAG = ConfigOptions
            .key(TAG)
            .stringType()
            .noDefaultValue()
            .withDescription("Rocketmq tag.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> FORMAT = FactoryUtil.FORMAT;

    public static final ConfigOption<List<String>> OPTION_KEY_FIELDS =
            ConfigOptions.key(KEY_FIELDS)
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> OPTION_CONSUMER_TIMEOUT = ConfigOptions
            .key(CONSUMER_TIMEOUT)
            .intType()
            .defaultValue(DEFAULT_CONSUMER_TIMEOUT)
            .withDescription("Consumer scan timeout.");

    public static final ConfigOption<ScanStartupMode> OPTION_SCAN_STARTUP_MODE = ConfigOptions
            .key(SCAN_STARTUP_MODE)
            .enumType(ScanStartupMode.class)
            .defaultValue(ScanStartupMode.GROUP_OFFSETS)
            .withDescription("Startup mode for RocketMQ consumer.");

    public static final ConfigOption<String> OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS = ConfigOptions
            .key(SCAN_STARTUP_SPECIFIC_OFFSETS)
            .stringType()
            .noDefaultValue()
            .withDescription("Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<Long> OPTION_SCAN_STARTUP_TIMESTAMP_MILLIS = ConfigOptions
            .key(SCAN_STARTUP_TIMESTAMP_MILLIS)
            .longType()
            .noDefaultValue()
            .withDescription("Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Long> OPTION_PARTITION_DISCOVERY_INTERVAL_MS = ConfigOptions
            .key(SCAN_TOPIC_PARTITION_DISCOVERY_INTERVAL_MS)
            .longType()
            .noDefaultValue()
            .withDescription(
                    "Optional interval for consumer to discover dynamically created Kafka partitions periodically.");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> OPTION_PRODUCER_TIMEOUT = ConfigOptions
            .key(PRODUCER_TIMEOUT)
            .intType()
            .defaultValue(DEFAULT_PRODUCER_TIMEOUT)
            .withDescription("Producer send records timeout.");

    public static final ConfigOption<String> OPTION_SINK_PARTITIONER = ConfigOptions
            .key(SINK_PARTITIONER)
            .stringType()
            .defaultValue("default")
            .withDescription(
                    Description.builder()
                            .text(
                                    "Optional output partitioning from Flink's partitions into Rocketmq's partitions. Valid enumerations are")
                            .list(
                                    text(
                                            "'default' (use Rocketmq default partitioner to partition records)"),
                                    text(
                                            "'fixed' (each Flink partition ends up in at most one Rocketmq partition)"),
                                    text(
                                            "'round-robin' (a Flink partition is distributed to Kafka partitions round-robin when 'key.fields' is not specified)"),
                                    text(
                                            "custom class name (use custom FlinkKafkaPartitioner subclass)"))
                            .build());

    // Disable this feature by default
    public static final ConfigOption<Integer> OPTION_SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key(SINK_BUFFER_FLUSH_MAX_ROWS)
            .intType()
            .defaultValue(DEFAULT_SINK_BUFFER_FLUSH_MAX_ROWS)
            .withDescription(
                    Description.builder()
                            .text(
                                    "The max size of buffered records before flushing. "
                                            + "When the sink receives many updates on the same key, "
                                            + "the buffer will retain the last records of the same key. "
                                            + "This can help to reduce data shuffling and avoid possible tombstone messages to the Kafka topic.")
                            .linebreak()
                            .text("Can be set to '0' to disable it.")
                            .linebreak()
                            .text(
                                    "Note both 'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval' "
                                            + "must be set to be greater than zero to enable sink buffer flushing.")
                            .build());

    // Disable this feature by default
    public static final ConfigOption<Duration> OPTION_SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key(SINK_BUFFER_FLUSH_INTERVAL)
            .durationType()
            .defaultValue(DEFAULT_SINK_BUFFER_FLUSH_INTERVAL)
            .withDescription(
                    Description.builder()
                            .text(
                                    "The flush interval millis. Over this time, asynchronous threads "
                                            + "will flush data. When the sink receives many updates on the same key, "
                                            + "the buffer will retain the last record of the same key.")
                            .linebreak()
                            .text("Can be set to '0' to disable it.")
                            .linebreak()
                            .text(
                                    "Note both 'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval' "
                                            + "must be set to be greater than zero to enable sink buffer flushing.")
                            .build());

    public static final ConfigOption<DeliveryGuarantee> OPTION_DELIVERY_GUARANTEE = ConfigOptions
            .key(SINK_DELIVERY_GUARANTEE)
            .enumType(DeliveryGuarantee.class)
            .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
            .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<String> OPTION_TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key(SINK_TRANSACTIONAL_ID_PREFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If the delivery guarantee is configured as "
                                    + DeliveryGuarantee.EXACTLY_ONCE
                                    + " this value is used a prefix for the identifier of all opened Kafka transactions.");

    // --------------------------------------------------------------------------------------------
    // Access
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> OPTION_ACCESS_KEY = ConfigOptions
            .key(ACCESS_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Access Key.");

    public static final ConfigOption<String> OPTION_SECRET_KEY = ConfigOptions
            .key(SECRET_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Secret Key.");


    public enum ScanStartupMode implements DescribedEnum {

        EARLIEST_OFFSET("earliest-offset", text("Start from the earliest offset possible.")),

        LATEST_OFFSET("latest-offset", text("Start from the latest offset.")),

        GROUP_OFFSETS(
                "group-offsets",
                text(
                        "Start from committed offsets in ZooKeeper / Kafka brokers of a specific consumer group.")),

        TIMESTAMP("timestamp", text("Start from user-supplied timestamp for each partition.")),

        SPECIFIC_OFFSETS(
                "specific-offsets",
                text("Start from user-supplied specific offsets for each partition."));

        private final String value;
        private final InlineElement description;

        ScanStartupMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
