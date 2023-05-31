package kai.lu.rocketmq.flink.source.reader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import kai.lu.rocketmq.flink.source.reader.deserializer.RocketMQDeserializationSchema;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SplitReader} implementation that reads records from RocketMQ partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 */
public class RocketMQPartitionSplitReader<T>
        implements SplitReader<Tuple3<T, Long, Long>, RocketMQPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQPartitionSplitReader.class);

    private final String topic;
    private final String tag;
    private final String sql;
    private final long stopInMs;
    private final long startTime;
    private final long startOffset;

    private final int consumerTimeout;

    private final RocketMQDeserializationSchema<T> deserializationSchema;
    private final Map<Tuple3<String, String, Integer>, Long> startingOffsets;
    private final Map<Tuple3<String, String, Integer>, Long> stoppingTimestamps;
    private final SimpleCollector<T> collector;

    private DefaultLitePullConsumer consumer;

    private volatile boolean wakeup = false;

    private static final int MAX_MESSAGE_NUMBER_PER_BLOCK = 64;

    public RocketMQPartitionSplitReader(
            String topic,
            String consumerGroup,
            int consumerTimeout,
            String nameServerAddress,
            String accessKey,
            String secretKey,
            String tag,
            String sql,
            long stopInMs,
            long startTime,
            long startOffset,
            RocketMQDeserializationSchema<T> deserializationSchema) {

        this.topic = topic;
        this.tag = tag;
        this.sql = sql;
        this.stopInMs = stopInMs;
        this.startTime = startTime;
        this.startOffset = startOffset;
        this.deserializationSchema = deserializationSchema;
        this.startingOffsets = new HashMap<>();
        this.stoppingTimestamps = new HashMap<>();
        this.collector = new SimpleCollector<>();
        this.consumerTimeout = consumerTimeout;

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

            Tuple3<String, String, Integer> topicPartition = new Tuple3<>(
                    lastMessageExt.getTopic(),
                    lastMessageExt.getBrokerName(),
                    lastMessageExt.getQueueId()
            );

            startingOffsets.put(topicPartition, lastMessageExt.getQueueOffset());

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

                long stoppingTimestamp = getStoppingTimestamp(topicPartition);
                long storeTimestamp = messageExt.getStoreTimestamp();
                if (storeTimestamp > stoppingTimestamp) {
                    finishSplitAtRecord(
                            topicPartition,
                            stoppingTimestamp,
                            messageExt.getQueueOffset(),
                            recordsBySplits
                    );
                    break;
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
                .forEach(split -> {
                    Tuple3<String, String, Integer> topicPartition = new Tuple3<>(
                            split.getTopic(),
                            split.getBroker(),
                            split.getPartition()
                    );

                    startingOffsets.put(topicPartition, split.getStartingOffset());
                    stoppingTimestamps.put(topicPartition, split.getStoppingTimestamp());
                });

        LOG.info("Rocketmq needs to assign queue firstly, queues: " + startingOffsets);

        Map<MessageQueue, Long> queueMap = new HashMap<>();
        for (Map.Entry<Tuple3<String, String, Integer>, Long> entry : startingOffsets.entrySet()) {
            Tuple3<String, String, Integer> topicPartition = entry.getKey();
            MessageQueue messageQueue = new MessageQueue(topicPartition.f0, topicPartition.f1, topicPartition.f2);
            queueMap.put(messageQueue, entry.getValue());
        }

        consumer.assign(queueMap.keySet());
        for (Map.Entry<MessageQueue, Long> entry : queueMap.entrySet()) {
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

    private void finishSplitAtRecord(
            Tuple3<String, String, Integer> topicPartition,
            long stoppingTimestamp,
            long currentOffset,
            RocketMQPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits) {

        LOG.debug("{} has reached stopping timestamp {}, current offset is {}",
                topicPartition.f0 + "-" + topicPartition.f1, stoppingTimestamp, currentOffset);
        recordsBySplits.addFinishedSplit(RocketMQPartitionSplit.toSplitId(topicPartition));
        startingOffsets.remove(topicPartition);
        stoppingTimestamps.remove(topicPartition);
    }

    private long getStoppingTimestamp(Tuple3<String, String, Integer> topicPartition) {
        return stoppingTimestamps.getOrDefault(topicPartition, stopInMs);
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

//            if (StringUtils.isNotEmpty(sql)) {
//                consumer.subscribe(topic, MessageSelector.bySql(sql));
//            } else {
//                consumer.subscribe(topic, tag);
//            }
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

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
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
        public void close() {}

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }
}
