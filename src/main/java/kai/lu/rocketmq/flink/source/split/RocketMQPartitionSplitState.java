package kai.lu.rocketmq.flink.source.split;

/** This class extends RocketMQPartitionSplit to track a mutable current offset. */
public class RocketMQPartitionSplitState extends RocketMQPartitionSplit {

    private long currentOffset;

    public RocketMQPartitionSplitState(RocketMQPartitionSplit partitionSplit) {
        super(
                partitionSplit.getTopic(),
                partitionSplit.getBroker(),
                partitionSplit.getPartition(),
                partitionSplit.getStartingOffset(),
                partitionSplit.getStoppingTimestamp()
        );
        this.currentOffset = partitionSplit.getStartingOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    /**
     * Use the current offset as the starting offset to create a new RocketMQPartitionSplit.
     *
     * @return a new RocketMQPartitionSplit which uses the current offset as its starting offset.
     */
    public RocketMQPartitionSplit toRocketMQPartitionSplit() {
        return new RocketMQPartitionSplit(
                getTopic(),
                getBroker(),
                getPartition(),
                getCurrentOffset(),
                getStoppingTimestamp()
        );
    }
}
