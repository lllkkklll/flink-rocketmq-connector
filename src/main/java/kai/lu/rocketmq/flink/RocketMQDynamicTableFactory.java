package kai.lu.rocketmq.flink;

import kai.lu.rocketmq.flink.common.RocketMQOptions;
import kai.lu.rocketmq.flink.sink.table.RocketMQDynamicTableSink;
import kai.lu.rocketmq.flink.source.table.RocketMQScanTableSource;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static kai.lu.rocketmq.flink.common.RocketMQOptions.*;
import static kai.lu.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 *  flink sql rocketmq connector
 *
 *  This is a common usage of flink sql connector, it implements both TableSource and TableSink.
 *
 */
public class RocketMQDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "rocketmq";
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {

        return helper.discoverOptionalDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseThrow(() -> new RuntimeException("Not found format."));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {

        return helper.discoverOptionalEncodingFormat(
                SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseThrow(() -> new RuntimeException("Not found format."));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = getValueDecodingFormat(helper);
        final DataType physicalDataType = context.getPhysicalRowDataType();

        helper.validate();

        final Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String topic = configuration.getString(TOPIC);
        String consumerGroup = configuration.getString(GROUP_ID);
        int consumerTimeout = configuration.getInteger(OPTIONAL_CONSUMER_TIMEOUT);
        String nameServerAddress = configuration.getString(NAME_SERVER_ADDRESS);
        String tag = configuration.getString(OPTIONAL_TAG);
        String sql = configuration.getString(OPTIONAL_SQL);

        if (configuration.contains(OPTIONAL_SCAN_STARTUP_MODE)
                && (configuration.contains(OPTIONAL_START_MESSAGE_OFFSET)
                || configuration.contains(OPTIONAL_START_TIME_MILLS)
                || configuration.contains(OPTIONAL_START_TIME))) {
            throw new IllegalArgumentException(
                    String.format(
                            "cannot support these configs when %s has been set: [%s, %s, %s] !",
                            OPTIONAL_SCAN_STARTUP_MODE.key(),
                            OPTIONAL_START_MESSAGE_OFFSET.key(),
                            OPTIONAL_START_TIME.key(),
                            OPTIONAL_START_TIME_MILLS.key()));
        }
        long startMessageOffset = configuration.getLong(OPTIONAL_START_MESSAGE_OFFSET);
        long startTimeMs = configuration.getLong(OPTIONAL_START_TIME_MILLS);
        String startDateTime = configuration.getString(OPTIONAL_START_TIME);
        String timeZone = configuration.getString(OPTIONAL_TIME_ZONE);
        String accessKey = configuration.getString(OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(OPTIONAL_SECRET_KEY);
        long startTime = startTimeMs;
        if (startTime == -1) {
            if (!StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
                try {
                    startTime = parseDateString(startDateTime, timeZone);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Incorrect datetime format: %s, pls use ISO-8601 "
                                            + "complete date plus hours, minutes and seconds format:%s.",
                                    startDateTime, DATE_FORMAT),
                            e);
                }
            }
        }
        long stopInMs = Long.MAX_VALUE;

        String endDateTime = configuration.getString(OPTIONAL_END_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(endDateTime)) {
            try {
                stopInMs = parseDateString(endDateTime, timeZone);
            } catch (ParseException e) {
                throw new RuntimeException(
                        String.format(
                                "Incorrect datetime format: %s, pls use ISO-8601 "
                                        + "complete date plus hours, minutes and seconds format:%s.",
                                endDateTime, DATE_FORMAT),
                        e);
            }
            Preconditions.checkArgument(
                    stopInMs >= startTime, "Start time should be less than stop time.");
        }
        long partitionDiscoveryIntervalMs = configuration.getLong(OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        boolean useNewApi = configuration.getBoolean(OPTIONAL_USE_NEW_API);

        String consumerOffsetMode = configuration.getString(
                RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE, CONSUMER_OFFSET_LATEST);
        long consumerOffsetTimestamp = configuration.getLong(
                RocketMQOptions.OPTIONAL_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis());

        return createRocketMQTableSource(
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
                startMessageOffset < 0 ? startTime : -1L,
                partitionDiscoveryIntervalMs,
                consumerOffsetMode,
                consumerOffsetTimestamp,
                useNewApi,
                context.getObjectIdentifier().asSummaryString());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = getValueEncodingFormat(helper);
        final DataType physicalDataType = context.getPhysicalRowDataType();
        helper.validate();

        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration properties = Configuration.fromMap(rawProperties);
        String topic = properties.getString(TOPIC);
        String producerGroup = properties.getString(GROUP_ID);
        int producerTimeout = properties.getInteger(OPTIONAL_PRODUCER_TIMEOUT);
        String nameServerAddress = properties.getString(NAME_SERVER_ADDRESS);

        String accessKey = properties.getString(OPTIONAL_ACCESS_KEY);
        String secretKey = properties.getString(OPTIONAL_SECRET_KEY);

        String tag = properties.getString(OPTIONAL_TAG);
//        String dynamicColumn = properties.getString(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
//
//        boolean writeKeysToBody = properties.getBoolean(OPTIONAL_WRITE_KEYS_TO_BODY);
//        String keyColumnsConfig = properties.getString(OPTIONAL_WRITE_KEY_COLUMNS);
//        String[] keyColumns = new String[0];
//
//        if (keyColumnsConfig != null && keyColumnsConfig.length() > 0) {
//            keyColumns = keyColumnsConfig.split(",");
//        }

        final Integer parallelism = tableOptions.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);

        return createRocketMQTableSink(
                physicalDataType,
                valueEncodingFormat,
                topic,
                producerGroup,
                producerTimeout,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                parallelism
        );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(TOPIC);
        requiredOptions.add(GROUP_ID);
        requiredOptions.add(NAME_SERVER_ADDRESS);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        optionalOptions.add(FactoryUtil.FORMAT);
        optionalOptions.add(OPTIONAL_TAG);
        optionalOptions.add(OPTIONAL_SQL);
        optionalOptions.add(OPTIONAL_START_MESSAGE_OFFSET);
        optionalOptions.add(OPTIONAL_START_TIME_MILLS);
        optionalOptions.add(OPTIONAL_START_TIME);
        optionalOptions.add(OPTIONAL_END_TIME);
        optionalOptions.add(OPTIONAL_TIME_ZONE);
        optionalOptions.add(OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        optionalOptions.add(OPTIONAL_USE_NEW_API);
        optionalOptions.add(OPTIONAL_ACCESS_KEY);
        optionalOptions.add(OPTIONAL_SECRET_KEY);
        optionalOptions.add(OPTIONAL_SCAN_STARTUP_MODE);

        optionalOptions.add(OPTIONAL_PRODUCER_TIMEOUT);
        optionalOptions.add(OPTIONAL_CONSUMER_TIMEOUT);

        // 写入配置
        optionalOptions.add(OPTIONAL_WRITE_RETRY_TIMES);
        optionalOptions.add(OPTIONAL_WRITE_SLEEP_TIME_MS);
        optionalOptions.add(OPTIONAL_WRITE_IS_DYNAMIC_TAG);
        optionalOptions.add(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
        optionalOptions.add(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED);
        optionalOptions.add(OPTIONAL_WRITE_KEYS_TO_BODY);
        optionalOptions.add(OPTIONAL_WRITE_KEY_COLUMNS);

        // 文件写入格式设置
        optionalOptions.add(ENCODING);
        optionalOptions.add(FIELD_DELIMITER);
        optionalOptions.add(LINE_DELIMITER);
        optionalOptions.add(COLUMN_ERROR_DEBUG);
        optionalOptions.add(LENGTH_CHECK);

        return optionalOptions;
    }

    // --------------------------------------------------------------------------------------------
    private RocketMQScanTableSource createRocketMQTableSource(
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
        return new RocketMQScanTableSource(
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
                startMessageOffset < 0 ? startTime : -1L,
                partitionDiscoveryIntervalMs,
                consumerOffsetMode,
                consumerOffsetTimestamp,
                useNewApi,
                tableIdentifier);
    }

    private RocketMQDynamicTableSink createRocketMQTableSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            String topic,
            String producerGroup,
            long producerTimeout,
            String nameServerAddress,
            String accessKey,
            String secretKey,
            String tag,
            Integer parallelism) {
        return new RocketMQDynamicTableSink(
                physicalDataType,
                encodingFormat,
                topic,
                producerGroup,
                producerTimeout,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                parallelism);
    }

    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat = FastDateFormat.getInstance(
                DATE_FORMAT, TimeZone.getTimeZone(timeZone));

        return simpleDateFormat.parse(dateString).getTime();
    }
}
