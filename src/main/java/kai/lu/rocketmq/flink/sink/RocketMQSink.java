package kai.lu.rocketmq.flink.sink;

import kai.lu.rocketmq.flink.sink.serializer.RocketMQSerializationSchema;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.Duration;
import java.util.Properties;

public class RocketMQSink<IN>
        implements StatefulSink<IN, RocketMQWriterState>,
        TwoPhaseCommittingSink<IN, RocketMQCommitter> {

    private final DeliveryGuarantee deliveryGuarantee;

    private final RocketMQSerializationSchema<IN> messageSerializer;
    private final String transactionalIdPrefix;
    private final String topic;
    private final String nameServerAddress;
    private final String producerGroup;
    private final String tag;
    private final String accessKey;
    private final String secretKey;
    private final long producerTimeout;
    private final String partitioner;
    private final int bufferFlushMaxRows;
    private final Duration bufferFlushInterval;

    RocketMQSink(
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            RocketMQSerializationSchema<IN> messageSerializer,
            String topic,
            String nameServerAddress,
            String producerGroup,
            String tag,
            String accessKey,
            String secretKey,
            long producerTimeout,
            String partitioner,
            int bufferFlushMaxRows,
            Duration bufferFlushInterval) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.messageSerializer = messageSerializer;
        this.topic = topic;
        this.nameServerAddress = nameServerAddress;
        this.producerGroup = producerGroup;
        this.tag = tag;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.producerTimeout = producerTimeout;
        this.partitioner = partitioner;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushInterval = bufferFlushInterval;
    }

    /**
     * Create a {@link RocketMQSinkBuilder} to construct a new {@link RocketMQSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link RocketMQSinkBuilder}
     */
    public static <IN> RocketMQSinkBuilder<IN> builder() {
        return new RocketMQSinkBuilder<>();
    }


}
