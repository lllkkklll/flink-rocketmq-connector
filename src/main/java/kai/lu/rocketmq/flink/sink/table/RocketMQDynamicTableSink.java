package kai.lu.rocketmq.flink.sink.table;

import kai.lu.rocketmq.flink.legacy.RocketMQConfig;
import kai.lu.rocketmq.flink.legacy.RocketMQSink;
import kai.lu.rocketmq.flink.sink.serializer.DynamicRocketMQMessageSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static kai.lu.rocketmq.flink.sink.table.RocketMQRowDataConverter.MetadataConverter;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Defines the dynamic table sink of RocketMQ.
 */
public class RocketMQDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Parallelism of the physical Kafka producer. *
     */
    protected final @Nullable
    Integer parallelism;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------
    /**
     * Data type to configure the formats.
     */
    private final DataType physicalDataType;

    /**
     * Format for encoding values to RocketMQ.
     */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    // --------------------------------------------------------------------------------------------
    // specific attributes
    // --------------------------------------------------------------------------------------------
    private final String topic;
    private final String producerGroup;
    private final long producerTimeout;
    private final String nameServerAddress;

    private final String accessKey;
    private final String secretKey;

    private final String tag;
    /**
     * Metadata that is appended at the end of a physical sink row.
     */
    private List<String> metadataKeys;

    public RocketMQDynamicTableSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            String topic,
            String producerGroup,
            long producerTimeout,
            String nameServerAddress,
            String accessKey,
            String secretKey,
            String tag,
            @Nullable Integer parallelism) {

        // Format attributes
        this.physicalDataType = checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.encodingFormat = checkNotNull(encodingFormat, "Value encoding format must not be null.");

        this.topic = topic;
        this.producerGroup = producerGroup;
        this.producerTimeout = producerTimeout;
        this.nameServerAddress = nameServerAddress;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.tag = tag;

        this.metadataKeys = Collections.emptyList();
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
        SerializationSchema<RowData> serializationSchema = createSerialization(context, encodingFormat);

        DynamicRocketMQMessageSerializationSchema schema = new DynamicRocketMQMessageSerializationSchema(
                serializationSchema, topic, tag);

        return SinkFunctionProvider.of(
                new RocketMQRowDataSink(
                        new RocketMQSink(getProducerProps()), schema
                ), parallelism);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public DynamicTableSink copy() {
        RocketMQDynamicTableSink tableSink = new RocketMQDynamicTableSink(
                physicalDataType,
                encodingFormat,
                topic,
                producerGroup,
                producerTimeout,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                parallelism
        );

        tableSink.metadataKeys = metadataKeys;

        return tableSink;
    }

    @Override
    public String asSummaryString() {
        return "Rocketmq table sink";
    }

    // --------------------------------------------------------------------------------------------

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format) {
        if (format == null) {
            return null;
        }

        return format.createRuntimeEncoder(context, physicalDataType);
    }

    private Properties getProducerProps() {
        Properties producerProps = new Properties();

        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, producerGroup);
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameServerAddress);
        producerProps.setProperty(RocketMQConfig.PRODUCER_TIMEOUT, String.valueOf(producerTimeout));

        if (accessKey != null && secretKey != null) {
            producerProps.setProperty(RocketMQConfig.ACCESS_KEY, accessKey);
            producerProps.setProperty(RocketMQConfig.SECRET_KEY, secretKey);
        }

        return producerProps;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum WritableMetadata {
        KEYS(
                "keys",
                DataTypes.STRING().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                }),

        TAGS(
                "tags",
                DataTypes.STRING().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
