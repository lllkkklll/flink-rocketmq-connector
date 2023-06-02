package kai.lu.rocketmq.flink.source;

import kai.lu.rocketmq.flink.option.RocketMQConnectorOptions;
import kai.lu.rocketmq.flink.source.enumerator.RocketMQSourceEnumState;
import kai.lu.rocketmq.flink.source.enumerator.RocketMQSourceEnumStateSerializer;
import kai.lu.rocketmq.flink.source.enumerator.RocketMQSourceEnumerator;
import kai.lu.rocketmq.flink.source.reader.RocketMQPartitionSplitReader;
import kai.lu.rocketmq.flink.source.reader.RocketMQRecordEmitter;
import kai.lu.rocketmq.flink.source.reader.RocketMQSourceReader;
import kai.lu.rocketmq.flink.source.reader.deserializer.RocketMQDeserializationSchema;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplitSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The Source implementation of RocketMQ.
 */
public class RocketMQSource<OUT>
        implements Source<OUT, RocketMQPartitionSplit, RocketMQSourceEnumState>,
        ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = -1L;

    // Topic
    private final String topic;

    // Nameserver Address
    private final String nameServerAddress;

    // Consumer Group Id
    private final String consumerGroup;

    // Tag
    private final String tag;

    // Consumer message timeout
    private final int consumerTimeout;

    // Discover new partitions
    private final long partitionDiscoveryIntervalMs;

    // Access
    private final String accessKey;
    private final String secretKey;

    // Boundedness
    private final Boundedness boundedness;

    // Deserialization
    private final RocketMQDeserializationSchema<OUT> deserializationSchema;

    private final RocketMQConnectorOptions.ScanStartupMode startupMode;

    private final Map<MessageQueue, Long> specificStartupOffsets;

    private final long startupTimestampMillis;

    public RocketMQSource(
            String topic,
            String nameServerAddress,
            String consumerGroup,
            String tag,
            int consumerTimeout,
            long partitionDiscoveryIntervalMs,
            String accessKey,
            String secretKey,
            RocketMQDeserializationSchema<OUT> deserializationSchema,
            RocketMQConnectorOptions.ScanStartupMode startupMode,
            Map<MessageQueue, Long> specificStartupOffsets,
            long startupTimestampMillis) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.consumerTimeout = consumerTimeout;
        this.nameServerAddress = nameServerAddress;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.tag = tag;
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.deserializationSchema = deserializationSchema;
        this.startupMode = startupMode;
        this.specificStartupOffsets = specificStartupOffsets;
        this.startupTimestampMillis = startupTimestampMillis;
    }

    /**
     * Get a RocketMQSourceBuilder to build a {@link RocketMQSource}.
     *
     * @return a RocketMQ source builder.
     */
    public static <OUT> RocketMQSourceBuilder<OUT> builder() {
        return new RocketMQSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, RocketMQPartitionSplit> createReader(
            SourceReaderContext readerContext) throws Exception {

        FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<OUT, Long, Long>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return null;
                    }
                });

        Supplier<SplitReader<Tuple3<OUT, Long, Long>, RocketMQPartitionSplit>> splitReaderSupplier = () ->
                new RocketMQPartitionSplitReader<>(
                        topic,
                        nameServerAddress,
                        consumerGroup,
                        tag,
                        consumerTimeout,
                        accessKey,
                        secretKey,
                        deserializationSchema);
        RocketMQRecordEmitter<OUT> recordEmitter = new RocketMQRecordEmitter<>();

        return new RocketMQSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                new Configuration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext) {

        return new RocketMQSourceEnumerator(
                topic,
                nameServerAddress,
                consumerGroup,
                accessKey,
                secretKey,
                partitionDiscoveryIntervalMs,
                boundedness,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                enumContext);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext,
            RocketMQSourceEnumState checkpoint) {

        return new RocketMQSourceEnumerator(
                topic,
                nameServerAddress,
                consumerGroup,
                accessKey,
                secretKey,
                partitionDiscoveryIntervalMs,
                boundedness,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                enumContext,
                checkpoint.getCurrentAssignment());
    }

    @Override
    public SimpleVersionedSerializer<RocketMQPartitionSplit> getSplitSerializer() {
        return new RocketMQPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<RocketMQSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new RocketMQSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
