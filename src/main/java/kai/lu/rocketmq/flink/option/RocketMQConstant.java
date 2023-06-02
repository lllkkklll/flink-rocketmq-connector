package kai.lu.rocketmq.flink.option;

import java.time.Duration;

public class RocketMQConstant {
    // Access control config
    public static final String ACCESS_KEY = "access.key";
    public static final String SECRET_KEY = "secret.key";
    public static final String TIME_ZONE = "timeZone";

    // Rocketmq specific options
    public static final String PROPERTIES_PREFIX = "properties.";
    public static final String PROPERTIES_NAMESERVER_ADDRESS = "properties.nameserver.address"; // Required

    public static final String PROPERTIES_TOPIC = "properties.topic"; // Required
    public static final String PROPERTIES_GROUP_ID = "properties.group.id"; // Required
    public static final String TAG = "tag";
    public static final String KEY = "key";

    // Source options
    public static final String CONSUMER_TIMEOUT = "consumer.timeout";
    public static final int DEFAULT_CONSUMER_TIMEOUT = 30000; // 30 seconds
    public static final String SCAN_STARTUP_MODE = "scan.startup.mode";
    public static final String SCAN_STARTUP_SPECIFIC_OFFSETS = "scan.startup.specific-offsets";
    public static final String SCAN_STARTUP_TIMESTAMP_MILLIS = "scan.startup.timestamp-millis";
    public static final String SCAN_TOPIC_PARTITION_DISCOVERY_INTERVAL_MS = "scan.topic-partition-discovery.interval-millis";

    // Sink Options
    public static final String KEY_FIELDS = "key.fields";
    public static final String SINK_PARTITIONER = "sink.partitioner";
    public static final String SINK_BUFFER_FLUSH_MAX_ROWS = "sink.buffer-flush.max-rows";
    public static final int DEFAULT_SINK_BUFFER_FLUSH_MAX_ROWS = 0;
    public static final String SINK_BUFFER_FLUSH_INTERVAL = "sink.buffer-flush.interval";
    public static final Duration DEFAULT_SINK_BUFFER_FLUSH_INTERVAL = Duration.ofSeconds(0);
    public static final String SINK_DELIVERY_GUARANTEE = "sink.delivery-guarantee";
    public static final String SINK_TRANSACTIONAL_ID_PREFIX = "sink.transactional-id-prefix";

    public static final String PRODUCER_TIMEOUT = "producer.timeout";
    public static final int DEFAULT_PRODUCER_TIMEOUT = 30000; // 30 seconds
}
