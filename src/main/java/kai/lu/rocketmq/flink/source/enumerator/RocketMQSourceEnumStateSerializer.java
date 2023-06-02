package kai.lu.rocketmq.flink.source.enumerator;

import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplitSerializer;

import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of RocketMQ source. */
public class RocketMQSourceEnumStateSerializer
        implements SimpleVersionedSerializer<RocketMQSourceEnumState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQSourceEnumState enumState) throws IOException {
        return SerdeUtils.serializeSplitAssignments(
                enumState.getCurrentAssignment(),
                new RocketMQPartitionSplitSerializer()
        );
    }

    @Override
    public RocketMQSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // Check whether the version of serialized bytes is supported.
        if (version == getVersion()) {
            Map<Integer, List<RocketMQPartitionSplit>> currentPartitionAssignment =
                    SerdeUtils.deserializeSplitAssignments(
                            serialized, new RocketMQPartitionSplitSerializer(), ArrayList::new);
            return new RocketMQSourceEnumState(currentPartitionAssignment);
        }
        throw new IOException(String.format(
                "The bytes are serialized with version %d, while this deserializer only supports version up to %d",
                version,
                getVersion()
        ));
    }
}
