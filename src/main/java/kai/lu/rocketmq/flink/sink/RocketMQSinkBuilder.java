package kai.lu.rocketmq.flink.sink;

import kai.lu.rocketmq.flink.sink.serializer.RocketMQSerializationSchema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class RocketMQSinkBuilder<IN> {

    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;

    private String transactionalIdPrefix = "rocketmq-sink";

    private RocketMQSerializationSchema<IN> messageSerializer;

    private String topic;

    private String nameserver;

    private String producerGroupId;

    private String tag;

    private String accessKey;

    private String secretKey;

    private long producerTimeout;

    private int bufferFlushMaxRows;

    private Duration bufferFlushInterval;

    RocketMQSinkBuilder() {}

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setDeliverGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Sets the {@link RocketMQSerializationSchema} that transforms incoming records to {@link
     * org.apache.rocketmq.common.message.Message}s.
     *
     * @param messageSerializer
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setMessageSerializer(
            RocketMQSerializationSchema<IN> messageSerializer) {
        this.messageSerializer = checkNotNull(messageSerializer, "messageSerializer");
        ClosureCleaner.clean(
                this.messageSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Sets the prefix for all created transactionalIds if {@link DeliveryGuarantee#EXACTLY_ONCE} is
     * configured.
     *
     * <p>It is mandatory to always set this value with {@link DeliveryGuarantee#EXACTLY_ONCE} to
     * prevent corrupted transactions if multiple jobs using the RocketMQSink run against the same
     * RocketMQ Cluster. The default prefix is {@link #transactionalIdPrefix}.
     *
     * <p>The size of the prefix is capped by {@link #MAXIMUM_PREFIX_BYTES} formatted with UTF-8.
     *
     * <p>It is important to keep the prefix stable across application restarts. If the prefix
     * changes it might happen that lingering transactions are not correctly aborted and newly
     * written messages are not immediately consumable until the transactions timeout.
     *
     * @param transactionalIdPrefix
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        checkState(
                transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                        <= MAXIMUM_PREFIX_BYTES,
                "The configured prefix is too long and the resulting transactionalId might exceed Kafka's transactionalIds size.");
        return this;
    }

    public RocketMQSinkBuilder<IN> setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public RocketMQSinkBuilder<IN> setNameserver(String nameserver) {
        this.nameserver = nameserver;
        return this;
    }

    public RocketMQSinkBuilder<IN> setProducerGroupId(String producerGroupId) {
        this.producerGroupId = producerGroupId;
        return this;
    }

    public RocketMQSinkBuilder<IN> setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public RocketMQSinkBuilder<IN> setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public RocketMQSinkBuilder<IN> setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public RocketMQSinkBuilder<IN> setProducerTimeout(long consumerTimeout) {
        this.producerTimeout = producerTimeout;
        return this;
    }

    public RocketMQSinkBuilder<IN> setBufferFlushMaxRows(int bufferFlushMaxRows) {
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        return this;
    }

    public RocketMQSinkBuilder<IN> setBufferFlushInterval(Duration bufferFlushInterval) {
        this.bufferFlushInterval = bufferFlushInterval;
        return this;
    }

    private void sanityCheck() {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkState(
                    transactionalIdPrefix != null,
                    "EXACTLY_ONCE delivery guarantee requires a transactionIdPrefix to be set to provide unique transaction names across multiple KafkaSinks writing to the same Kafka cluster.");
        }
        checkNotNull(messageSerializer, "recordSerializer");
    }

    /**
     * Constructs the {@link RocketMQSink} with the configured properties.
     *
     * @return {@link RocketMQSink}
     */
    public RocketMQSink<IN> build() {
        sanityCheck();

        return new RocketMQSink<>(
                deliveryGuarantee,
                transactionalIdPrefix,
                messageSerializer,
                topic,
                nameserver,
                producerGroupId,
                tag,
                accessKey,
                secretKey,
                producerTimeout,
                bufferFlushMaxRows,
                bufferFlushInterval);
    }
}
