package kai.lu.rocketmq.flink.source.enumerator;

import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;

import java.util.List;
import java.util.Map;

/** The state of RocketMQ source enumerator. */
public class RocketMQSourceEnumState {

    private final Map<Integer, List<RocketMQPartitionSplit>> currentAssignment;

    RocketMQSourceEnumState(Map<Integer, List<RocketMQPartitionSplit>> currentAssignment) {
        this.currentAssignment = currentAssignment;
    }

    public Map<Integer, List<RocketMQPartitionSplit>> getCurrentAssignment() {
        return currentAssignment;
    }
}
