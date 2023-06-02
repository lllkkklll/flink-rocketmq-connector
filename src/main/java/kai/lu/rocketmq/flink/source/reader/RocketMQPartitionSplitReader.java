package kai.lu.rocketmq.flink.source.reader;

import kai.lu.rocketmq.flink.source.reader.deserializer.RocketMQDeserializationSchema;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * A {@link SplitReader} implementation that reads records from RocketMQ partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 */
public class RocketMQPartitionSplitReader<T>
        implements SplitReader<Tuple3<T, Long, Long>, RocketMQPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQPartitionSplitReader.class);
    private static final int MAX_MESSAGE_NUMBER_PER_BLOCK = 64;

    private final String topic;
    private final String tag;
    private final int consumerTimeout;
    private final RocketMQDeserializationSchema<T> deserializationSchema;
    private final Map<MessageQueue, Long> startingOffsets;

    private final SimpleCollector<T> collector;
    private DefaultLitePullConsumer consumer;
    private volatile boolean wakeup = false;

    public RocketMQPartitionSplitReader(
            String topic,
            String nameServerAddress,
            String consumerGroup,
            String tag,
            int consumerTimeout,
            String accessKey,
            String secretKey,
            RocketMQDeserializationSchema<T> deserializationSchema) {

        this.topic = topic;
        this.tag = tag;
        this.consumerTimeout = consumerTimeout;
        this.deserializationSchema = deserializationSchema;
        this.startingOffsets = new HashMap<>();
        this.collector = new SimpleCollector<>();

        initialRocketMQConsumer(consumerGroup, nameServerAddress, accessKey, secretKey);
    }

    @Override
    public RecordsWithSplitIds<Tuple3<T, Long, Long>> fetch() throws IOException {
        RocketMQPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits = new RocketMQPartitionSplitRecords<>();

        if (wakeup) {
            wakeup = false;
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }

        consumer.setPullBatchSize(MAX_MESSAGE_NUMBER_PER_BLOCK);
        List<MessageExt> messageExtList = consumer.poll(consumerTimeout);

        if (CollectionUtils.isNotEmpty(messageExtList)) {
            MessageExt lastMessageExt = messageExtList.get(messageExtList.size() - 1);

            MessageQueue messageQueue = new MessageQueue(
                    lastMessageExt.getTopic(),
                    lastMessageExt.getBrokerName(),
                    lastMessageExt.getQueueId());

            startingOffsets.put(messageQueue, lastMessageExt.getQueueOffset());

            // 处理数据
            Collection<Tuple3<T, Long, Long>> recordsForSplit = recordsBySplits.recordsForSplit(
                    lastMessageExt.getTopic() + "-" +
                            lastMessageExt.getBrokerName() + "-" +
                            lastMessageExt.getQueueId()
            );
            for (MessageExt messageExt : messageExtList) {
                if (StringUtils.isNotEmpty(tag) &&
                        StringUtils.isNotEmpty(messageExt.getTags()) &&
                        !messageExt.getTags().equals(tag)) {

                    continue;
                }
                // Add the record to the partition collector.
                try {
                    deserializationSchema.deserialize(messageExt.getBody(), collector);
                    collector.getRecords()
                            .forEach(
                                    r ->
                                            recordsForSplit.add(
                                                    new Tuple3<>(
                                                            r,
                                                            messageExt.getQueueOffset(),
                                                            messageExt.getStoreTimestamp())
                                            )
                            );
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize consumer record due to", e);
                } finally {
                    collector.reset();
                }
            }
        }
        recordsBySplits.prepareForRead();
        LOG.debug(
                String.format(
                        "Fetch record splits for MetaQ subscribe message queues of topic[%s].",
                        topic
                )
        );
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQPartitionSplit> splitsChange) {
        // Get all the partition assignments and stopping timestamps..
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()
                    )
            );
        }

        // Set up the stopping timestamps.
        splitsChange.splits()
                .forEach(split -> startingOffsets.put(split.getMessageQueue(), split.getStartingOffset()));

        LOG.info("Rocketmq needs to assign queue firstly, queues: " + startingOffsets);

        consumer.assign(startingOffsets.keySet());
        for (Map.Entry<MessageQueue, Long> entry : startingOffsets.entrySet()) {
            try {
                consumer.seek(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void wakeUp() {
        LOG.debug("Wake up the split reader in case the fetcher thread is blocking in fetch().");
        wakeup = true;
    }

    @Override
    public void close() {
        consumer.shutdown();
    }

    // --------------- private helper method ----------------------

    private void initialRocketMQConsumer(
            String consumerGroup,
            String nameServerAddress,
            String accessKey,
            String secretKey) {
        try {
            if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
                AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
                        new SessionCredentials(accessKey, secretKey)
                );
                consumer = new DefaultLitePullConsumer(consumerGroup, aclClientRPCHook);
            } else {
                consumer = new DefaultLitePullConsumer(consumerGroup);
            }
            consumer.setNamesrvAddr(nameServerAddress);
            consumer.setInstanceName(
                    String.join(
                            "||",
                            ManagementFactory.getRuntimeMXBean().getName(),
                            topic,
                            consumerGroup,
                            "" + System.nanoTime()));
            consumer.start();
        } catch (MQClientException e) {
            LOG.error("Failed to initial RocketMQ consumer.", e);
            consumer.shutdown();
        }
    }

    // ---------------- private helper class ------------------------

    private static class RocketMQPartitionSplitRecords<T> implements RecordsWithSplitIds<T> {

        private final Map<String, Collection<T>> recordsBySplits;
        private final Set<String> finishedSplits;
        private Iterator<Map.Entry<String, Collection<T>>> splitIterator;
        private String currentSplitId;
        private Iterator<T> recordIterator;

        public RocketMQPartitionSplitRecords() {
            this.recordsBySplits = new HashMap<>();
            this.finishedSplits = new HashSet<>();
        }

        private Collection<T> recordsForSplit(String splitId) {
            return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void prepareForRead() {
            splitIterator = recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<T>> entry = splitIterator.next();
                currentSplitId = entry.getKey();
                recordIterator = entry.getValue().iterator();
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
                return null;
            }
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                return recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {
        }

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }
}
