package kai.lu.rocketmq.flink.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.rocketmq.common.message.MessageQueue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer serializer} for {@link RocketMQPartitionSplit}. */
public class RocketMQPartitionSplitSerializer
        implements SimpleVersionedSerializer<RocketMQPartitionSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            MessageQueue messageQueue = split.getMessageQueue();
            out.writeUTF(messageQueue.getTopic());
            out.writeUTF(messageQueue.getBrokerName());
            out.writeInt(messageQueue.getQueueId());
            out.writeLong(split.getStartingOffset());
            out.writeLong(split.getStoppingTimestamp());
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public RocketMQPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String topic = in.readUTF();
            String broker = in.readUTF();
            int partition = in.readInt();

            MessageQueue messageQueue = new MessageQueue(topic, broker, partition);

            long offset = in.readLong();
            long timestamp = in.readLong();

            return new RocketMQPartitionSplit(messageQueue, offset, timestamp);
        }
    }
}
