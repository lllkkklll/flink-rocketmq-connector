package kai.lu.rocketmq.flink.legacy.common.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface KeyValueDeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {
    T deserializeKeyAndValue(byte[] key, byte[] value);
}
