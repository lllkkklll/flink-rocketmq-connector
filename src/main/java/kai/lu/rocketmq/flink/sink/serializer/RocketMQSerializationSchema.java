package kai.lu.rocketmq.flink.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import org.apache.rocketmq.common.message.Message;

/**
 * 行转成RocketMq对应的Message, 用于输出信息
 */
@PublicEvolving
public interface RocketMQSerializationSchema<IN> extends Serializable {

    Message serialize(IN element);
}
