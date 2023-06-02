package kai.lu.rocketmq.flink;

import kai.lu.rocketmq.flink.sink.RocketMQDynamicTableSink;
import kai.lu.rocketmq.flink.sink.partitioner.FlinkRocketMQPartitioner;
import kai.lu.rocketmq.flink.source.RocketMQScanTableSource;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static kai.lu.rocketmq.flink.option.RocketMQConnectorOptions.*;
import static kai.lu.rocketmq.flink.option.RocketMQConstant.PROPERTIES_PREFIX;
import static kai.lu.rocketmq.flink.util.RocketMQConnectorOptionsUtil.*;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * flink sql rocketmq connector
 * <p>
 * This is a common usage of flink sql connector, it implements both TableSource and TableSink.
 */
public class RocketMQDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQDynamicTableFactory.class);
    private static final ConfigOption<String> SINK_SEMANTIC =
            ConfigOptions.key("sink.semantic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional semantic when committing.");
    private static final String IDENTIFIER = "rocketmq";

    private static DecodingFormat<DeserializationSchema<RowData>> getDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {

        return helper.discoverOptionalDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseThrow(() -> new RuntimeException("Not found format."));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {

        return helper.discoverOptionalEncodingFormat(
                SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseThrow(() -> new RuntimeException("Not found format."));
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            Map<String, String> options,
            Format format) {
        if (primaryKeyIndexes.length > 0 && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration configuration = Configuration.fromMap(options);
            String formatName = configuration.getOptional(FactoryUtil.FORMAT)
                    .orElseThrow(() -> new RuntimeException("Please set format firstly."));
            throw new ValidationException(
                    String.format(
                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    private static DeliveryGuarantee validateDeprecatedSemantic(ReadableConfig tableOptions) {
        if (tableOptions.getOptional(SINK_SEMANTIC).isPresent()) {
            LOG.warn(
                    "{} is deprecated and will be removed. Please use {} instead.",
                    SINK_SEMANTIC.key(),
                    OPTION_DELIVERY_GUARANTEE.key());
            return DeliveryGuarantee.valueOf(
                    tableOptions.get(SINK_SEMANTIC).toUpperCase().replace("-", "_"));
        }
        return tableOptions.get(OPTION_DELIVERY_GUARANTEE);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                getDecodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validateTableSourceOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                decodingFormat);

        final StartupOptions startupOptions = getStartupOptions(tableOptions);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        return createRocketMQTableSource(
                physicalDataType,
                decodingFormat,
                getTopic(tableOptions),
                getNameserver(tableOptions),
                getGroupId(tableOptions),
                getTag(tableOptions),
                getAccessKey(tableOptions),
                getSecretKey(tableOptions),
                getConsumerTimeout(tableOptions),
                getPartitionDiscoveryInterval(tableOptions),
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis,
                context.getObjectIdentifier().asSummaryString());
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = getEncodingFormat(helper);

        final ReadableConfig tableOptions = helper.getOptions();

        final DeliveryGuarantee deliveryGuarantee = validateDeprecatedSemantic(tableOptions);
        validateTableSinkOptions(tableOptions);

        validateDeliveryGuarantee(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                encodingFormat);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(1);

        return createRocketMQTableSink(
                physicalDataType,
                encodingFormat,
                getTopic(tableOptions),
                getNameserver(tableOptions),
                getGroupId(tableOptions),
                getTag(tableOptions),
                getAccessKey(tableOptions),
                getSecretKey(tableOptions),
                tableOptions.get(OPTION_PRODUCER_TIMEOUT),
                tableOptions.get(OPTION_SINK_BUFFER_FLUSH_MAX_ROWS),
                tableOptions.get(OPTION_SINK_BUFFER_FLUSH_INTERVAL),
                getFlinkRocketMQPartitioner(tableOptions, context.getClassLoader()).orElse(null),
                deliveryGuarantee,
                parallelism,
                tableOptions.get(OPTION_TRANSACTIONAL_ID_PREFIX)
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();

        options.add(OPTION_PROPERTIES_NAMESERVER_ADDRESS);
        options.add(OPTION_PROPERTIES_TOPIC);
        options.add(OPTION_PROPERTIES_GROUP_ID);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();

        options.add(FORMAT);
        options.add(OPTION_TAG);
        options.add(OPTION_PARTITION_DISCOVERY_INTERVAL_MS);
        options.add(OPTION_CONSUMER_TIMEOUT);
        options.add(OPTION_SCAN_STARTUP_MODE);
        options.add(OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(OPTION_SCAN_STARTUP_TIMESTAMP_MILLIS);

        options.add(OPTION_PRODUCER_TIMEOUT);
        options.add(OPTION_SINK_PARTITIONER);
        options.add(OPTION_SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(OPTION_SINK_BUFFER_FLUSH_INTERVAL);
        options.add(OPTION_DELIVERY_GUARANTEE);
        options.add(OPTION_TRANSACTIONAL_ID_PREFIX);

        options.add(OPTION_ACCESS_KEY);
        options.add(OPTION_SECRET_KEY);

        return options;
    }

    // --------------------------------------------------------------------------------------------
    private RocketMQScanTableSource createRocketMQTableSource(
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
            ScanStartupMode startupMode,
            Map<MessageQueue, Long> specificStartupOffsets,
            long startupTimestampMillis,
            String tableIdentifier) {
        return new RocketMQScanTableSource(
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
                tableIdentifier);
    }

    private RocketMQDynamicTableSink createRocketMQTableSink(
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
            FlinkRocketMQPartitioner<RowData> flinkRocketMQPartitioner,
            DeliveryGuarantee deliveryGuarantee,
            Integer parallelism,
            @Nullable String transactionalIdPrefix) {
        return new RocketMQDynamicTableSink(
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
                transactionalIdPrefix);
    }
}
