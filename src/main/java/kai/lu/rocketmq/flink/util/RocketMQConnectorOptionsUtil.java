package kai.lu.rocketmq.flink.util;

import kai.lu.rocketmq.flink.sink.partitioner.FlinkFixedPartitioner;
import kai.lu.rocketmq.flink.sink.partitioner.FlinkRocketMQPartitioner;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

import static kai.lu.rocketmq.flink.option.RocketMQConnectorOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

public class RocketMQConnectorOptionsUtil {

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_DEFAULT = "default";

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";
    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";
    protected static final String AVRO_CONFLUENT = "avro-confluent";
    protected static final String DEBEZIUM_AVRO_CONFLUENT = "debezium-avro-confluent";
    private static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
            ConfigOptions.key("schema-registry.subject").stringType().noDefaultValue();
    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";
    private static final List<String> SCHEMA_REGISTRY_FORMATS =
            Arrays.asList(AVRO_CONFLUENT, DEBEZIUM_AVRO_CONFLUENT);


    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    private RocketMQConnectorOptionsUtil() {
    }

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateTopic(tableOptions);
        validateScanStartupMode(tableOptions);
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateTopic(tableOptions);
        validateSinkPartitioner(tableOptions);
    }

    public static void validateTopic(ReadableConfig tableOptions) {
        Optional<String> topic = tableOptions.getOptional(OPTION_PROPERTIES_TOPIC);

        if (!topic.isPresent()) {
            throw new ValidationException("Either 'topic' must be set.");
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(OPTION_SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(OPTION_SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode " +
                                                                "but missing.",
                                                        OPTION_SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        ScanStartupMode.TIMESTAMP
                                                ));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!tableOptions
                                            .getOptional(OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode " +
                                                                "but missing.",
                                                        OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                        ScanStartupMode.SPECIFIC_OFFSETS
                                                ));
                                    }
                                    String specificOffsets = tableOptions.get(OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    parseSpecificOffsets(
                                            specificOffsets,
                                            OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS.key());

                                    break;
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static void validateSinkPartitioner(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(OPTION_SINK_PARTITIONER)
                .ifPresent(
                        partitioner -> {
                            if (partitioner.equals(SINK_PARTITIONER_VALUE_ROUND_ROBIN)
                                    && tableOptions.getOptional(OPTION_KEY_FIELDS).isPresent()) {
                                throw new ValidationException(
                                        "Currently 'round-robin' partitioner only works when option 'key.fields' is not specified.");
                            } else if (partitioner.isEmpty()) {
                                throw new ValidationException(
                                        String.format(
                                                "Option '%s' should be a non-empty string.",
                                                OPTION_SINK_PARTITIONER.key()));
                            }
                        });
    }

    public static String getTopic(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_PROPERTIES_TOPIC).orElse(null);
    }

    public static String getNameserver(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_PROPERTIES_NAMESERVER_ADDRESS).orElse(null);
    }

    public static String getGroupId(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_PROPERTIES_GROUP_ID).orElse(null);
    }

    public static String getTag(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_TAG).orElse(null);
    }

    public static String getAccessKey(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_ACCESS_KEY).orElse(null);
    }

    public static String getSecretKey(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_SECRET_KEY).orElse(null);
    }

    public static long getPartitionDiscoveryInterval(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_PARTITION_DISCOVERY_INTERVAL_MS)
                .orElse(-1L);
    }

    public static int getConsumerTimeout(ReadableConfig tableOptions) {
        return tableOptions.getOptional(OPTION_CONSUMER_TIMEOUT).orElse(-1);
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final Map<MessageQueue, Long> specificOffsets = new HashMap<>();
        final ScanStartupMode startupMode = tableOptions.getOptional(OPTION_SCAN_STARTUP_MODE)
                .orElse(ScanStartupMode.GROUP_OFFSETS);

        if (startupMode == ScanStartupMode.SPECIFIC_OFFSETS) {
            buildSpecificOffsets(tableOptions, tableOptions.get(OPTION_PROPERTIES_TOPIC), specificOffsets);
        }

        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == ScanStartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(OPTION_SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static void buildSpecificOffsets(
            ReadableConfig tableOptions,
            String topic,
            Map<MessageQueue, Long> specificOffsets) {
        String specificOffsetsStrOpt = tableOptions.get(OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap = parseSpecificOffsets(specificOffsetsStrOpt, OPTION_SCAN_STARTUP_SPECIFIC_OFFSETS.key());
        offsetMap.forEach(
                (partition, offset) -> {
                    final MessageQueue topicPartition = new MessageQueue(topic, null, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    /**
     * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class
     * name.
     */
    public static Optional<FlinkRocketMQPartitioner<RowData>> getFlinkRocketMQPartitioner(
            ReadableConfig tableOptions, ClassLoader classLoader) {
        return tableOptions
                .getOptional(OPTION_SINK_PARTITIONER)
                .flatMap(
                        (String partitioner) -> {
                            switch (partitioner) {
                                case SINK_PARTITIONER_VALUE_FIXED:
                                    return Optional.of(new FlinkFixedPartitioner<>());
                                case SINK_PARTITIONER_VALUE_DEFAULT:
                                case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                                    return Optional.empty();
                                // Default fallback to full class name of the partitioner.
                                default:
                                    return Optional.of(
                                            initializePartitioner(partitioner, classLoader));
                            }
                        });
    }

    public static Properties getRocketMQProperties(Map<String, String> tableOptions) {
        final Properties properties = new Properties();

        if (hasKafkaClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                properties.put(subKey, value);
                            });
        }
        return properties;
    }

    /**
     * Parses SpecificOffsets String to Map.
     *
     * <p>SpecificOffsets String format was given as following:
     *
     * <pre>
     *     scan.startup.specific-offsets = partition:0,offset:42;partition:1,offset:300
     * </pre>
     *
     * @return SpecificOffsets with Map format, key is partition, and value is offset
     */
    public static Map<Integer, Long> parseSpecificOffsets(
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage = String.format(
                "Invalid properties '%s' should follow the format "
                        + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                optionKey,
                specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2
                    || !kv[0].startsWith(PARTITION + ':')
                    || !kv[1].startsWith(OFFSET + ':')) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
    }

    /** Returns a class value with the given class name. */
    private static <T> FlinkRocketMQPartitioner<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!FlinkRocketMQPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink partitioner class '%s' should extend from the required class %s",
                                name, FlinkRocketMQPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final FlinkRocketMQPartitioner<T> kafkaPartitioner =
                    InstantiationUtil.instantiate(name, FlinkRocketMQPartitioner.class, classLoader);

            return kafkaPartitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    /**
     * Returns a new table context with a default schema registry subject value in the options if
     * the format is a schema registry format (e.g. 'avro-confluent') and the subject is not
     * defined.
     */
    public static DynamicTableFactory.Context autoCompleteSchemaRegistrySubject(
            DynamicTableFactory.Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        Map<String, String> newOptions = autoCompleteSchemaRegistrySubject(tableOptions);
        if (newOptions.size() > tableOptions.size()) {
            // build a new context
            return new FactoryUtil.DefaultDynamicTableContext(
                    context.getObjectIdentifier(),
                    context.getCatalogTable().copy(newOptions),
                    context.getEnrichmentOptions(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else {
            return context;
        }
    }

    private static Map<String, String> autoCompleteSchemaRegistrySubject(Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        // the subject autoComplete should only be used in sink, check the topic first
        validateTopic(configuration);
        final Optional<String> format = configuration.getOptional(FORMAT);
        final String topic = configuration.get(OPTION_PROPERTIES_TOPIC);

        if (format.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(format.get())) {
            autoCompleteSubject(configuration, format.get(), topic + "-value");
        }

        return configuration.toMap();
    }

    private static void autoCompleteSubject(
            Configuration configuration, String format, String subject) {
        ConfigOption<String> subjectOption = ConfigOptions
                .key(format + "." + SCHEMA_REGISTRY_SUBJECT.key())
                .stringType()
                .noDefaultValue();
        if (!configuration.getOptional(subjectOption).isPresent()) {
            configuration.setString(subjectOption, subject);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    public static void validateDeliveryGuarantee(ReadableConfig tableOptions) {
        if (tableOptions.get(OPTION_DELIVERY_GUARANTEE) == DeliveryGuarantee.EXACTLY_ONCE
                && !tableOptions.getOptional(OPTION_TRANSACTIONAL_ID_PREFIX).isPresent()) {
            throw new ValidationException(
                    OPTION_TRANSACTIONAL_ID_PREFIX.key()
                            + " must be specified when using DeliveryGuarantee.EXACTLY_ONCE.");
        }
    }

    /**
     * Kafka startup options. *
     */
    public static class StartupOptions {
        public ScanStartupMode startupMode;
        public Map<MessageQueue, Long> specificOffsets;
        public long startupTimestampMillis;
    }
}
