package kai.lu.rocketmq.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Objects;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQPartitionSplit implements SourceSplit {

    private final MessageQueue messageQueue;
    private final long startingOffset;

    public RocketMQPartitionSplit(
            MessageQueue messageQueue,
            long startingOffset) {

        this.messageQueue = messageQueue;
        this.startingOffset = startingOffset;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    @Override
    public String splitId() {
        return messageQueue.toString();
    }

    @Override
    public String toString() {
        return String.format(
                "[Topic: %s, Broker: %s, Partition: %s, StartingOffset: %d, StoppingTimestamp: %d]",
                messageQueue.getTopic(),
                messageQueue.getBrokerName(),
                messageQueue.getQueueId(),
                startingOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageQueue.getTopic(),
                messageQueue.getBrokerName(),
                messageQueue.getQueueId(),
                startingOffset
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RocketMQPartitionSplit)) {
            return false;
        }
        RocketMQPartitionSplit other = (RocketMQPartitionSplit) obj;
        return messageQueue.equals(other.messageQueue)
                && startingOffset == other.startingOffset;
    }

    public static String toSplitId(MessageQueue messageQueue) {
        return messageQueue.getTopic() +
                "-" + messageQueue.getBrokerName() +
                "-" + messageQueue.getQueueId();
    }
}
