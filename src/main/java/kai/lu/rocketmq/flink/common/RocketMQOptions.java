package kai.lu.rocketmq.flink.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static kai.lu.rocketmq.flink.legacy.RocketMQConfig.*;

public class RocketMQOptions {

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("Topic name of the RocketMQ record.");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("groupId")
            .stringType()
            .noDefaultValue()
            .withDescription("Consumer and Product group.");

    public static final ConfigOption<Integer> OPTIONAL_PRODUCER_TIMEOUT = ConfigOptions
            .key("producer.timeout")
            .intType()
            .defaultValue(DEFAULT_PRODUCER_TIMEOUT)
            .withDescription("Producer send records timeout.");

    public static final ConfigOption<Integer> OPTIONAL_CONSUMER_TIMEOUT = ConfigOptions
            .key("consumer.timeout")
            .intType()
            .defaultValue(DEFAULT_CONSUMER_TIMEOUT)
            .withDescription("Consumer scan timeout.");

    public static final ConfigOption<String> NAME_SERVER_ADDRESS = ConfigOptions
            .key("nameServerAddress")
            .stringType()
            .noDefaultValue()
            .withDescription("name server address Required.");

    public static final ConfigOption<String> OPTIONAL_TAG = ConfigOptions
            .key("tag")
            .stringType()
            .noDefaultValue()
            .withDescription("consumer topic tag.");

    public static final ConfigOption<String> OPTIONAL_SQL = ConfigOptions
            .key("sql")
            .stringType()
            .noDefaultValue()
            .withDescription("consumer topic sql.");

    public static final ConfigOption<String> OPTIONAL_SCAN_STARTUP_MODE = ConfigOptions
            .key("scanStartupMode")
            .stringType()
            .defaultValue("latest")
            .withDescription("Consumer scan policies: earliest, latest, timestamp or offset(long).");

    public static final ConfigOption<Long> OPTIONAL_OFFSET_FROM_TIMESTAMP = ConfigOptions
            .key("offsetFromTimestamp")
            .longType()
            .noDefaultValue()
            .withDescription("Consumer scan policy(timestamp): set timestamp interval.");

    public static final ConfigOption<Long> OPTIONAL_START_MESSAGE_OFFSET = ConfigOptions
            .key("startMessageOffset")
            .longType()
            .defaultValue(DEFAULT_START_MESSAGE_OFFSET)
            .withDescription("Consumer scan from defined offset.");

    public static final ConfigOption<Long> OPTIONAL_START_TIME_MILLS = ConfigOptions
            .key("startTimeMs")
            .longType()
            .defaultValue(-1L)
            .withDescription("Consumer scan from millisecond time.");

    public static final ConfigOption<String> OPTIONAL_START_TIME = ConfigOptions
            .key("startTime")
            .stringType()
            .noDefaultValue()
            .withDescription("Consumer scan from time(yyyy-MM-dd HH:mm:ss).");

    public static final ConfigOption<String> OPTIONAL_END_TIME = ConfigOptions
            .key("endTime")
            .stringType()
            .noDefaultValue()
            .withDescription("Consumer scan  end time(yyyy-MM-dd HH:mm:ss).");

    public static final ConfigOption<String> OPTIONAL_TIME_ZONE = ConfigOptions
            .key("timeZone")
            .stringType()
            .noDefaultValue()
            .withDescription("Time Zone.");

    public static final ConfigOption<Long> OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS = ConfigOptions
            .key("partitionDiscoveryIntervalMs")
            .longType()
            .defaultValue(30000L)
            .withDescription("Partition Discovery Interval(Ms).");

    public static final ConfigOption<Boolean> OPTIONAL_USE_NEW_API = ConfigOptions
            .key("useNewApi")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether use new Api.");

//    public static final ConfigOption<String> OPTIONAL_ENCODING = ConfigOptions
//            .key("encoding")
//            .stringType()
//            .defaultValue("UTF-8")
//            .withDescription("Encoding.");
//
//    public static final ConfigOption<String> OPTIONAL_FIELD_DELIMITER = ConfigOptions
//            .key("fieldDelimiter")
//            .stringType()
//            .defaultValue("\u0001")
//            .withDescription("Filed delimiter.");
//
//    public static final ConfigOption<String> OPTIONAL_LINE_DELIMITER = ConfigOptions
//            .key("lineDelimiter")
//            .stringType()
//            .defaultValue("\n")
//            .withDescription("Line delimiter.");

//    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG = ConfigOptions
//            .key("columnErrorDebug")
//            .booleanType()
//            .defaultValue(true)
//            .withDescription("Whether column error debug.");

//    public static final ConfigOption<String> OPTIONAL_LENGTH_CHECK = ConfigOptions
//            .key("lengthCheck")
//            .stringType()
//            .defaultValue("NONE")
//            .withDescription("Length check.");

    public static final ConfigOption<Integer> OPTIONAL_WRITE_RETRY_TIMES = ConfigOptions
            .key("retryTimes")
            .intType()
            .defaultValue(10)
            .withDescription("Retry times when it occurs exception or error.");

    public static final ConfigOption<Long> OPTIONAL_WRITE_SLEEP_TIME_MS = ConfigOptions
            .key("sleepTimeMs")
            .longType()
            .defaultValue(5000L)
            .withDescription("Sleep time(Ms), default value is 5000.");

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_TAG = ConfigOptions
            .key("isDynamicTag")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether dynamic tag.");

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN = ConfigOptions
            .key("dynamicTagColumn")
            .stringType()
            .noDefaultValue()
            .withDescription("Dynamic tag column.");

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED = ConfigOptions
            .key("dynamicTagColumnWriteIncluded")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether write including dynamic tag column.");

    public static final ConfigOption<String> OPTIONAL_WRITE_KEY_COLUMNS = ConfigOptions
            .key("keyColumns")
            .stringType()
            .noDefaultValue()
            .withDescription("Key Columns, split by ','.");

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_KEYS_TO_BODY = ConfigOptions
            .key("writeKeysToBody")
            .booleanType()
            .defaultValue(false)
            .withDescription("Write keys to body.");

    public static final ConfigOption<String> OPTIONAL_ACCESS_KEY = ConfigOptions
            .key("accessKey")
            .stringType()
            .noDefaultValue()
            .withDescription("Access Key.");

    public static final ConfigOption<String> OPTIONAL_SECRET_KEY = ConfigOptions
            .key("secretKey")
            .stringType()
            .noDefaultValue()
            .withDescription("Secret Key.");

    public static final ConfigOption<String> ENCODING = ConfigOptions
            .key("encoding".toLowerCase())
            .stringType()
            .defaultValue("UTF-8");

    public static final ConfigOption<String> FIELD_DELIMITER = ConfigOptions
            .key("fieldDelimiter".toLowerCase())
            .stringType()
            .defaultValue("\u0001");

    public static final ConfigOption<String> LINE_DELIMITER = ConfigOptions
            .key("lineDelimiter".toLowerCase())
            .stringType()
            .defaultValue("\n");

    public static final ConfigOption<Boolean> COLUMN_ERROR_DEBUG = ConfigOptions
            .key("columnErrorDebug".toLowerCase())
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<String> LENGTH_CHECK = ConfigOptions
            .key("lengthCheck".toLowerCase())
            .stringType()
            .defaultValue("NONE");
}
