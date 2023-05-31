package kai.lu.rocketmq.flink.legacy.common.serialization;

import org.apache.rocketmq.common.message.MessageExt;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * The interface Message ext deserialization scheme.
 *
 * @param <T> the type parameter
 */
public interface MessageExtDeserializationScheme<T> extends ResultTypeQueryable<T>, Serializable {
    /**
     * Deserialize messageExt to type T you want to output.
     *
     * @param messageExt the messageExt
     * @return the t
     */
    T deserializeMessageExt(MessageExt messageExt);
}
