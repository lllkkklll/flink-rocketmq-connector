package kai.lu.rocketmq.flink.legacy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Properties;
import java.util.UUID;

import static kai.lu.rocketmq.flink.legacy.common.util.RocketMQUtils.getAccessChannel;
import static kai.lu.rocketmq.flink.legacy.common.util.RocketMQUtils.getInteger;

/** RocketMQConfig for Consumer/Producer. */
public class RocketMQConfig {
    // Server Config
    public static final String NAME_SERVER_ADDR = "nameserver.address"; // Required

    public static final String NAME_SERVER_POLL_INTERVAL = "nameserver.poll.interval";
    public static final int DEFAULT_NAME_SERVER_POLL_INTERVAL = 30000; // 30 seconds

    public static final String BROKER_HEART_BEAT_INTERVAL = "brokerserver.heartbeat.interval";
    public static final int DEFAULT_BROKER_HEART_BEAT_INTERVAL = 30000; // 30 seconds

    // Access control config
    public static final String ACCESS_KEY = "access.key";
    public static final String SECRET_KEY = "secret.key";

    public static final String ACCESS_CHANNEL = "access.channel";
    public static final AccessChannel DEFAULT_ACCESS_CHANNEL = AccessChannel.LOCAL;

    // Producer related config
    public static final String PRODUCER_TOPIC = "producer.topic";
    public static final String PRODUCER_GROUP = "producer.group";

    public static final String PRODUCER_RETRY_TIMES = "producer.retry.times";
    public static final int DEFAULT_PRODUCER_RETRY_TIMES = 3;

    public static final String PRODUCER_TIMEOUT = "producer.timeout";

    public static final String CONSUMER_TIMEOUT = "consumer.timeout";
    public static final int DEFAULT_PRODUCER_TIMEOUT = 30000; // 3 seconds

    public static final int DEFAULT_CONSUMER_TIMEOUT = 30000; // 3 seconds

    // Consumer related config
    public static final String CONSUMER_GROUP = "consumer.group"; // Required
    public static final String CONSUMER_TOPIC = "consumer.topic"; // Required

    public static final String CONSUMER_TAG = "consumer.tag";
    public static final String CONSUMER_SQL = "consumer.sql";
    public static final String DEFAULT_CONSUMER_TAG = "*";

    public static final String CONSUMER_OFFSET_RESET_TO = "consumer.offset.reset.to";
    public static final String CONSUMER_OFFSET_LATEST = "latest";
    public static final String CONSUMER_OFFSET_EARLIEST = "earliest";
    public static final String CONSUMER_OFFSET_TIMESTAMP = "timestamp";
    public static final String CONSUMER_OFFSET_FROM_TIMESTAMP = "consumer.offset.from.timestamp";
    public static final String CONSUMER_OFFSET_PERSIST_INTERVAL = "consumer.offset.persist.interval";

    public static final int DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL = 5000; // 5 seconds

    public static final String CONSUMER_START_MESSAGE_OFFSET = "consumer.start.message.offset";

    public static final long DEFAULT_START_MESSAGE_OFFSET = 0;

    public static final String CONSUMER_BATCH_SIZE = "consumer.batch.size";
    public static final int DEFAULT_CONSUMER_BATCH_SIZE = 32;

    public static final String CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND =
            "consumer.delay.when.message.not.found";
    public static final int DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND = 100;

    public static final String CONSUMER_INDEX_OF_THIS_SUB_TASK = "consumer.index";

    public static final String UNIT_NAME = "unit.name";

    public static final String WATERMARK = "watermark";

    /**
     * Build Producer Configs.
     *
     * @param props Properties
     * @param producer DefaultMQProducer
     */
    public static void buildProducerConfigs(Properties props, DefaultMQProducer producer) {
        buildCommonConfigs(props, producer);
        String group = props.getProperty(PRODUCER_GROUP);
        if (StringUtils.isEmpty(group)) {
            group = UUID.randomUUID().toString();
        }
        producer.setProducerGroup(props.getProperty(PRODUCER_GROUP, group));
        producer.setRetryTimesWhenSendFailed(
                getInteger(props, PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setRetryTimesWhenSendAsyncFailed(
                getInteger(props, PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setSendMsgTimeout(getInteger(props, PRODUCER_TIMEOUT, DEFAULT_PRODUCER_TIMEOUT));
    }

    /**
     * Build Consumer Configs.
     *
     * @param props Properties
     * @param consumer DefaultLitePullConsumer
     */
    public static void buildConsumerConfigs(Properties props, DefaultLitePullConsumer consumer) {
        buildCommonConfigs(props, consumer);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPersistConsumerOffsetInterval(
                getInteger(
                        props,
                        CONSUMER_OFFSET_PERSIST_INTERVAL,
                        DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL));
    }

    /**
     * Build Common Configs.
     *
     * @param props Properties
     * @param client ClientConfig
     */
    public static void buildCommonConfigs(Properties props, ClientConfig client) {
        String nameServers = props.getProperty(NAME_SERVER_ADDR);
        Validate.notEmpty(nameServers);
        client.setNamesrvAddr(nameServers);
        client.setHeartbeatBrokerInterval(
                getInteger(props, BROKER_HEART_BEAT_INTERVAL, DEFAULT_BROKER_HEART_BEAT_INTERVAL));
        // When using aliyun products, you need to set up channels
        client.setAccessChannel(getAccessChannel(props, ACCESS_CHANNEL, DEFAULT_ACCESS_CHANNEL));
        client.setUnitName(props.getProperty(UNIT_NAME, null));
    }

    /**
     * Build credentials for client.
     *
     * @param props
     * @return
     */
    public static AclClientRPCHook buildAclRPCHook(Properties props) {
        String accessKey = props.getProperty(ACCESS_KEY);
        String secretKey = props.getProperty(SECRET_KEY);
        if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
            return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        return null;
    }
}
