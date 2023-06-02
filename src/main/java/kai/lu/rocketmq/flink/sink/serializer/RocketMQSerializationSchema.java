package kai.lu.rocketmq.flink.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rocketmq.common.message.Message;

/**
 * A serialization schema which defines how to convert a value of type {@code T} to {@link
 * Message}.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface RocketMQSerializationSchema<T> extends Serializable {


    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, KafkaSinkContext, Long)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    default void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {}

    Message serialize(T element);
}
