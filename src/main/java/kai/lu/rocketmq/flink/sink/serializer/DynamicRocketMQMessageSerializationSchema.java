package kai.lu.rocketmq.flink.sink.serializer;

import kai.lu.rocketmq.flink.sink.partitioner.FlinkRocketMQPartitioner;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.rocketmq.common.message.Message;

public class DynamicRocketMQMessageSerializationSchema implements RocketMQSerializationSchema<RowData> {

    private final String topic;

    private final FlinkRocketMQPartitioner<RowData> partitioner;

    private final SerializationSchema<RowData> serializationSchema;

    private final boolean hasMetadata;

    private final int[] metadataPositions;

    private final String tag;

    public DynamicRocketMQMessageSerializationSchema(
            String topic,
            FlinkRocketMQPartitioner<RowData> partitioner,
            SerializationSchema<RowData> serializationSchema,
            boolean hasMetadata,
            int[] metadataPositions,
            String tag) {
        this.topic = topic;
        this.partitioner = partitioner;
        this.serializationSchema = serializationSchema;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.tag = tag;
    }

    @Override
    public Message serialize(RowData element) {
        byte[] bytes = serializationSchema.serialize(element);

        Message message = new Message();
        message.setTopic(topic);
        if (tag != null && tag.length() > 0) {
            message.setTags(tag);
        }
        message.setBody(bytes);
        message.setWaitStoreMsgOK(true);

        return message;
    }
}
