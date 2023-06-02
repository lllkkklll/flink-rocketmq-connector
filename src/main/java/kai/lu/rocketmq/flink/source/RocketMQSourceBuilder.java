package kai.lu.rocketmq.flink.source;

import kai.lu.rocketmq.flink.option.RocketMQConnectorOptions;
import kai.lu.rocketmq.flink.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.Properties;

/**
 * The @builder class for {@link RocketMQSource} to make it easier for the users to construct a {@link
 * RocketMQSource}.
 *
 * <p>The following example shows the minimum setup to create a RocketMQSource that reads the String
 * values from a RocketMQ topic.
 *
 * <pre>{@code
 * RocketMQSource<String> source = RocketMQSource
 *     .<String>builder()
 *     .setNameserver(MY_NAMESERVER)
 *     .setTopic(TOPIC1)
 *     .setConsumerGroupId(MY_CONSUMER_GROUP_ID)
 *     .setDeserializer(RocketMQDeserializationSchema.class)
 *     .build();
 * }</pre>
 *
 * <p>The Nameserver, topic to consume, and the record deserializer are required fields that must be set.
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * RocketMQSource.
 */
public class RocketMQSourceBuilder<OUT> {

    private String topic;

    private String nameserver;

    private String consumerGroupId;

    private String tag;

    private String accessKey;

    private String secretKey;

    private int consumerTimeout;

    private long partitionDiscoveryIntervalMs;

    private RocketMQConnectorOptions.ScanStartupMode startupMode;

    private Map<MessageQueue, Long> specificStartupOffsets;

    private long startupTimestampMillis;

    private RocketMQDeserializationSchema<OUT> deserializationSchema;

    public RocketMQSourceBuilder<OUT> setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setNameserver(String nameserver) {
        this.nameserver = nameserver;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setConsumerTimeout(int consumerTimeout) {
        this.consumerTimeout = consumerTimeout;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setPartitionDiscoveryIntervalMs(
            long partitionDiscoveryIntervalMs) {
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setStartupMode(RocketMQConnectorOptions.ScanStartupMode startupMode) {
        this.startupMode = startupMode;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setSpecificStartupOffsets(Map<MessageQueue, Long> specificStartupOffsets) {
        this.specificStartupOffsets = specificStartupOffsets;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setStartupTimestampMillis(long startupTimestampMillis) {
        this.startupTimestampMillis = startupTimestampMillis;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setDeserializer(
            RocketMQDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Build the {@link RocketMQSource}.
     *
     * @return a RocketMQSource with the settings made for this builder.
     */
    public RocketMQSource<OUT> build() {
        return new RocketMQSource<>(
                topic,
                nameserver,
                consumerGroupId,
                tag,
                consumerTimeout,
                partitionDiscoveryIntervalMs,
                accessKey,
                secretKey,
                deserializationSchema,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis);
    }
}
