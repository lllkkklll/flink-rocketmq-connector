package kai.lu.rocketmq.flink.sink.partitioner;

import org.apache.flink.util.Preconditions;

/**
 * A partitioner ensuring that each internal Flink partition ends up in one RocketMQ partition.
 *
 * <p>Note, one RocketMQ partition can contain multiple Flink partitions.
 *
 * <p>There are a couple of cases to consider.
 *
 * <h3>More Flink partitions than rocketmq partitions</h3>
 *
 * <pre>
 * 		Flink Sinks:		RocketMQ Partitions
 * 			1	----------------&gt;	1
 * 			2   --------------/
 * 			3   -------------/
 * 			4	------------/
 * </pre>
 *
 * <p>Some (or all) rocketmq partitions contain the output of more than one flink partition
 *
 * <h3>Fewer Flink partitions than RocketMQ</h3>
 *
 * <pre>
 * 		Flink Sinks:		RocketMQ Partitions
 * 			1	----------------&gt;	1
 * 			2	----------------&gt;	2
 * 										3
 * 										4
 * 										5
 * </pre>
 *
 * <p>Not all RocketMQ partitions contain data To avoid such an unbalanced partitioning, use a
 * round-robin rocketmq partitioner (note that this will cause a lot of network connections between all
 * the Flink instances and all the RocketMQ brokers).
 */
public class FlinkFixedPartitioner<T> extends FlinkRocketMQPartitioner<T> {

    private static final long serialVersionUID = -1;

    private int parallelInstanceId;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");

        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");

        return partitions[parallelInstanceId % partitions.length];
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof FlinkFixedPartitioner;
    }

    @Override
    public int hashCode() {
        return FlinkFixedPartitioner.class.hashCode();
    }
}
