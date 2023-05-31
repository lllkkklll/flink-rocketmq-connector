package kai.lu.rocketmq.flink.sink.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.rocketmq.common.message.Message;

public class DynamicRocketMQMessageSerializationSchema implements RocketMQSerializationSchema<RowData> {

    private final SerializationSchema<RowData> serializationSchema;

    private final String topic;
    private final String tag;

    public DynamicRocketMQMessageSerializationSchema(
            SerializationSchema<RowData> serializationSchema,
            String topic,
            String tag) {

        this.serializationSchema = serializationSchema;
        this.topic = topic;
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
