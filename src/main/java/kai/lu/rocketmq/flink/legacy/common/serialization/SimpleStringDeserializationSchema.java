package kai.lu.rocketmq.flink.legacy.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/** deserialize the message body to string */
public class SimpleStringDeserializationSchema implements KeyValueDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String deserializeKeyAndValue(byte[] key, byte[] value) {
        String v = value != null ? new String(value, StandardCharsets.UTF_8) : null;
        return v;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
