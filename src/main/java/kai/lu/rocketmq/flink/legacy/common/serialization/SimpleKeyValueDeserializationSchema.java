package kai.lu.rocketmq.flink.legacy.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

public class SimpleKeyValueDeserializationSchema implements KeyValueDeserializationSchema<Map<String, String>> {

    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    private final String keyField;
    private final String valueField;

    public SimpleKeyValueDeserializationSchema() {
        this(DEFAULT_KEY_FIELD, DEFAULT_VALUE_FIELD);
    }

    /**
     * SimpleKeyValueDeserializationSchema Constructor.
     *
     * @param keyField tuple field for selecting the key
     * @param valueField tuple field for selecting the value
     */
    public SimpleKeyValueDeserializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public Map<String, String> deserializeKeyAndValue(byte[] key, byte[] value) {
        HashMap<String, String> map = new HashMap<>(2);
        if (keyField != null) {
            String k = key != null ? new String(key, StandardCharsets.UTF_8) : null;
            map.put(keyField, k);
        }
        if (valueField != null) {
            String v = value != null ? new String(value, StandardCharsets.UTF_8) : null;
            map.put(valueField, v);
        }
        return map;
    }

    @Override
    public TypeInformation<Map<String, String>> getProducedType() {
        return new MapTypeInfo<>(String.class, String.class);
    }
}
