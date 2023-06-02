package kai.lu.rocketmq.flink.sink.partitioner;

import java.io.Serializable;

/**
 * A {@link FlinkRocketMQPartitioner} wraps logic on how to partition records across partitions of
 * multiple RocketMQ topics.
 */
public abstract class FlinkRocketMQPartitioner <T> implements Serializable {

    private static final long serialVersionUID = -1;

    /**
     * Initializer for the partitioner. This is called once on each parallel sink instance of the
     * Flink Kafka producer. This method should be overridden if necessary.
     *
     * @param parallelInstanceId 0-indexed id of the parallel sink instance in Flink
     * @param parallelInstances the total number of parallel instances
     */
    public void open(int parallelInstanceId, int parallelInstances) {
        // overwrite this method if needed.
    }

    /**
     * Determine the id of the partition that the record should be written to.
     *
     * @param record the record value
     * @param key serialized key of the record
     * @param value serialized value of the record
     * @param targetTopic target topic for the record
     * @param partitions found partitions for the target topic
     * @return the id of the target partition
     */
    public abstract int partition(
            T record, byte[] key, byte[] value, String targetTopic, int[] partitions);
}
