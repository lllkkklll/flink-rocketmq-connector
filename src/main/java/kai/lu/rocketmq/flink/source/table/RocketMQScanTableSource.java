package kai.lu.rocketmq.flink.source.table;

import kai.lu.rocketmq.flink.source.RocketMQSource;
import kai.lu.rocketmq.flink.source.reader.deserializer.BytesMessage;
import kai.lu.rocketmq.flink.source.reader.deserializer.DynamicRocketMQDeserializationSchema;
import kai.lu.rocketmq.flink.source.reader.deserializer.RowDeserializationSchema.MetadataConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.flink.api.connector.source.Boundedness.BOUNDED;
import static org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED;

/**
 * Defines the scan table source of RocketMQ.
 */
public class RocketMQScanTableSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

    private static final String ROCKETMQ_TRANSFORMATION = "rocketmq";

    private final DataType physicalDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    private final String consumerOffsetMode;
    private final long consumerOffsetTimestamp;

    private final String topic;
    private final String consumerGroup;
    private final int consumerTimeout;
    private final String nameServerAddress;
    private final String tag;
    private final String sql;

    private final String accessKey;
    private final String secretKey;

    private final long stopInMs;
    private final long partitionDiscoveryIntervalMs;
    private final long startMessageOffset;
    private final long startTime;
    private final boolean useNewApi;
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
    private @Nullable
    WatermarkStrategy<RowData> watermarkStrategy;

    public RocketMQScanTableSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            String topic,
            String consumerGroup,
            int consumerTimeout,
            String nameServerAddress,
            String accessKey,
            String secretKey,
            String tag,
            String sql,
            long stopInMs,
            long startMessageOffset,
            long startTime,
            long partitionDiscoveryIntervalMs,
            String consumerOffsetMode,
            long consumerOffsetTimestamp,
            boolean useNewApi,
            String tableIdentifier) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");

        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.consumerTimeout = consumerTimeout;
        this.nameServerAddress = nameServerAddress;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.tag = tag;
        this.sql = sql;
        this.stopInMs = stopInMs;
        this.startMessageOffset = startMessageOffset;
        this.startTime = startTime;
        this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
        this.useNewApi = useNewApi;
        this.consumerOffsetMode = consumerOffsetMode;
        this.consumerOffsetTimestamp = consumerOffsetTimestamp;
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

        final DynamicRocketMQDeserializationSchema rocketMQDeserializer =
                new DynamicRocketMQDeserializationSchema(deserializationSchema, producedTypeInfo);

        boolean isBounded = stopInMs != Long.MAX_VALUE;

        RocketMQSource<RowData> rocketMQSource = new RocketMQSource<>(
                topic,
                consumerGroup,
                consumerTimeout,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                sql,
                stopInMs,
                startTime,
                startMessageOffset < 0 ? 0 : startMessageOffset,
                partitionDiscoveryIntervalMs,
                isBounded ? BOUNDED : CONTINUOUS_UNBOUNDED,
                rocketMQDeserializer,
                consumerOffsetMode,
                consumerOffsetTimestamp
        );

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
                return isBounded;
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
                consumerGroup,
                consumerTimeout,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                sql,
                stopInMs,
                startMessageOffset,
                startTime,
                partitionDiscoveryIntervalMs,
                consumerOffsetMode,
                consumerOffsetTimestamp,
                useNewApi,
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
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(BytesMessage message) {
                        return StringData.fromString(
                                String.valueOf(message.getProperty("__topic__")));
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
