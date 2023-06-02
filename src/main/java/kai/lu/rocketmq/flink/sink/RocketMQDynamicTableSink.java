package kai.lu.rocketmq.flink.sink;

import kai.lu.rocketmq.flink.sink.partitioner.FlinkRocketMQPartitioner;
import kai.lu.rocketmq.flink.sink.serializer.DynamicRocketMQMessageSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Define dynamic table sink of RocketMQ.
 */
public class RocketMQDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Parallelism of the physical Kafka producer. *
     */
    private final Integer parallelism;

    private final int bufferFlushMaxRows;

    private final Duration bufferFlushInterval;

    private final FlinkRocketMQPartitioner<RowData> partitioner;

    private final DeliveryGuarantee deliveryGuarantee;

    private final String transactionalIdPrefix;

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
            String nameServerAddress,
            String producerGroup,
            String tag,
            String accessKey,
            String secretKey,
            long producerTimeout,
            int bufferFlushMaxRows,
            Duration bufferFlushInterval,
            FlinkRocketMQPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            int parallelism,
            String transactionalIdPrefix) {

        // Format attributes
        this.physicalDataType = checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.encodingFormat = checkNotNull(encodingFormat, "Value encoding format must not be null.");

        this.topic = topic;
        this.nameServerAddress = nameServerAddress;
        this.producerGroup = producerGroup;
        this.tag = tag;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.producerTimeout = producerTimeout;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushInterval = bufferFlushInterval;
        this.deliveryGuarantee = deliveryGuarantee;
        this.partitioner = partitioner;
        this.parallelism = parallelism;
        this.transactionalIdPrefix = transactionalIdPrefix;

        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
        SerializationSchema<RowData> serializationSchema = createSerialization(context, encodingFormat);

        RocketMQSinkBuilder<RowData> sinkBuilder = RocketMQSink.builder();

        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();
        if (transactionalIdPrefix != null) {
            sinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        }

        final RocketMQSink<RowData> rocketMQSink = sinkBuilder
                .setDeliverGuarantee(deliveryGuarantee)
                .setTopic(topic)
                .setNameserver(nameServerAddress)
                .setProducerGroupId(producerGroup)
                .setTag(tag)
                .setAccessKey(accessKey)
                .setSecretKey(secretKey)
                .setProducerTimeout(producerTimeout)
                .setBufferFlushMaxRows(bufferFlushMaxRows)
                .setBufferFlushInterval(bufferFlushInterval)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setMessageSerializer(new DynamicRocketMQMessageSerializationSchema(
                        topic,
                        partitioner,
                        serializationSchema,
                        hasMetadata(),
                        getMetadataPositions(physicalChildren),
                        tag
                ))
                .build();

        return SinkV2Provider.of(rocketMQSink, parallelism);
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
                nameServerAddress,
                producerGroup,
                tag,
                accessKey,
                secretKey,
                producerTimeout,
                bufferFlushMaxRows,
                bufferFlushInterval,
                flinkRocketMQPartitioner,
                deliveryGuarantee,
                parallelism,
                transactionalIdPrefix
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

    private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = metadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            }
                            return physicalChildren.size() + pos;
                        })
                .toArray();
    }

    private boolean hasMetadata() {
        return metadataKeys.size() > 0;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum WritableMetadata {
        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        final MapData map = row.getMap(pos);
                        final ArrayData keyArray = map.keyArray();
                        final ArrayData valueArray = map.valueArray();
                        final List<Header> headers = new ArrayList<>();
                        for (int i = 0; i < keyArray.size(); i++) {
                            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                                final String key = keyArray.getString(i).toString();
                                final byte[] value = valueArray.getBinary(i);
                                headers.add(new RocketMQHeader(key, value));
                            }
                        }
                        return headers;
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getTimestamp(pos, 3).getMillisecond();
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

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }

    // --------------------------------------------------------------------------------------------

    private static class RocketMQHeader implements Header {

        private final String key;

        private final byte[] value;

        RocketMQHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }
    }
}
