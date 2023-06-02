package kai.lu.rocketmq.flink.source;

import kai.lu.rocketmq.flink.option.RocketMQConnectorOptions;
import kai.lu.rocketmq.flink.source.reader.deserializer.DynamicRocketMQDeserializationSchema;
import kai.lu.rocketmq.flink.source.reader.deserializer.DynamicRocketMQDeserializationSchema.MetadataConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

/**
 * Defines the scan table source of RocketMQ.
 */
public class RocketMQScanTableSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

    private static final String ROCKETMQ_TRANSFORMATION = "rocketmq";

    private final DataType physicalDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    private final String topic;

    private final String nameserver;

    private final String groupId;

    private final String tag;

    private final String accessKey;

    private final String secretKey;

    private final int consumerTimeout;

    private final long partitionDiscoveryIntervalMs;

    private final RocketMQConnectorOptions.ScanStartupMode startupMode;

    private final Map<MessageQueue, Long> specificStartupOffsets;

    private final long startupTimestampMillis;

    private final String tableIdentifier;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Data type that describes the final output of the source.
     */
    private DataType producedDataType;

    /**
     * Metadata that is appended at the end of a physical source row.
     */
    private List<String> metadataKeys;

    /**
     * Watermark strategy that is used to generate per-partition watermark.
     */
    @Nullable
    private WatermarkStrategy<RowData> watermarkStrategy;

    public RocketMQScanTableSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            String topic,
            String nameserver,
            String groupId,
            String tag,
            String accessKey,
            String secretKey,
            int consumerTimeout,
            long partitionDiscoveryIntervalMs,
            RocketMQConnectorOptions.ScanStartupMode startupMode,
            Map<MessageQueue, Long> specificStartupOffsets,
            long startupTimestampMillis,
            String tableIdentifier) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");

        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.nameserver = Preconditions.checkNotNull(nameserver, "Nameserver address must not be null.");
        this.groupId = Preconditions.checkNotNull(groupId, "Consumer group id must not be null.");
        this.tag = tag;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.consumerTimeout = consumerTimeout;
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
        this.startupMode =
                Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets =
                Preconditions.checkNotNull(
                        specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
        this.tableIdentifier = tableIdentifier;

        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();
        this.watermarkStrategy = null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> deserializationSchema = createDeserialization(context, valueDecodingFormat);
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);

        final RocketMQSource<RowData> rocketMQSource = createRocketMQSource(
                deserializationSchema, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                DataStreamSource<RowData> sourceStream = execEnv.fromSource(
                        rocketMQSource, watermarkStrategy, "RocketmqSource-" + tableIdentifier);
                providerContext.generateUid(ROCKETMQ_TRANSFORMATION).ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return rocketMQSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public boolean supportsMetadataProjection() {
        return false;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        RocketMQScanTableSource copy = new RocketMQScanTableSource(
                physicalDataType,
                valueDecodingFormat,
                topic,
                nameserver,
                groupId,
                tag,
                accessKey,
                secretKey,
                consumerTimeout,
                partitionDiscoveryIntervalMs,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                tableIdentifier
        );
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;

        return copy;
    }

    @Override
    public String asSummaryString() {
        return RocketMQScanTableSource.class.getName();
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format) {

        if (format == null) {
            return null;
        }
        return format.createRuntimeDecoder(context, this.physicalDataType);
    }

    // --------------------------------------------------------------------------------------------

    protected RocketMQSource<RowData> createRocketMQSource(
            DeserializationSchema<RowData> deserializationSchema,
            TypeInformation<RowData> producedTypeInfo) {

        final DynamicRocketMQDeserializationSchema rocketMQDeserializer =
                new DynamicRocketMQDeserializationSchema(deserializationSchema, producedTypeInfo);

        final RocketMQSourceBuilder<RowData> kafkaSourceBuilder = RocketMQSource.builder();

        return kafkaSourceBuilder.setTopic(topic)
                .setNameserver(nameserver)
                .setConsumerGroupId(groupId)
                .setTag(tag)
                .setConsumerTimeout(consumerTimeout)
                .setPartitionDiscoveryIntervalMs(partitionDiscoveryIntervalMs)
                .setAccessKey(accessKey)
                .setSecretKey(secretKey)
                .setStartupMode(startupMode)
                .setSpecificStartupOffsets(specificStartupOffsets)
                .setStartupTimestampMillis(startupTimestampMillis)
                .setDeserializer(rocketMQDeserializer)
                .build();
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(MessageExt message) {
                        return StringData.fromString(String.valueOf(message.getTopic()));
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
