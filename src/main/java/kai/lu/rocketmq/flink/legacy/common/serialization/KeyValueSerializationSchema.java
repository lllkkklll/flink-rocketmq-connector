package kai.lu.rocketmq.flink.legacy.common.serialization;

import java.io.Serializable;

public interface KeyValueSerializationSchema<T> extends Serializable {

    byte[] serializeKey(T tuple);

    byte[] serializeValue(T tuple);
}
