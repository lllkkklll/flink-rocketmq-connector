package kai.lu.rocketmq.flink.legacy.common.serialization;

import org.apache.rocketmq.common.message.MessageExt;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/** A Forward messageExt deserialization. */
public class ForwardMessageExtDeserialization
        implements MessageExtDeserializationScheme<MessageExt> {

    @Override
    public MessageExt deserializeMessageExt(MessageExt messageExt) {
        return messageExt;
    }

    @Override
    public TypeInformation<MessageExt> getProducedType() {
        return TypeInformation.of(MessageExt.class);
    }
}
