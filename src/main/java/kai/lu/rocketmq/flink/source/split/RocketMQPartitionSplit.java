package kai.lu.rocketmq.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Objects;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQPartitionSplit implements SourceSplit {

    private final String topic;
    private final String broker;
    private final int partition;
    private final long startingOffset;
    private final long stoppingTimestamp;

    public RocketMQPartitionSplit(
            String topic,
            String broker,
            int partition,
            long startingOffset,
            long stoppingTimestamp) {

        this.topic = topic;
        this.broker = broker;
        this.partition = partition;
        this.startingOffset = startingOffset;
        this.stoppingTimestamp = stoppingTimestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getBroker() {
        return broker;
    }

    public int getPartition() {
        return partition;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getStoppingTimestamp() {
        return stoppingTimestamp;
    }

    @Override
    public String splitId() {
        return topic + "-" + broker + "-" + partition;
    }

    @Override
    public String toString() {
        return String.format(
                "[Topic: %s, Broker: %s, Partition: %s, StartingOffset: %d, StoppingTimestamp: %d]",
                topic, broker, partition, startingOffset, stoppingTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, broker, partition, startingOffset, stoppingTimestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RocketMQPartitionSplit)) {
            return false;
        }
        RocketMQPartitionSplit other = (RocketMQPartitionSplit) obj;
        return topic.equals(other.topic)
                && broker.equals(other.broker)
                && partition == other.partition
                && startingOffset == other.startingOffset
                && stoppingTimestamp == other.stoppingTimestamp;
    }

    public static String toSplitId(Tuple3<String, String, Integer> topicPartition) {
        return topicPartition.f0 + "-" + topicPartition.f1 + "-" + topicPartition.f2;
    }
}
