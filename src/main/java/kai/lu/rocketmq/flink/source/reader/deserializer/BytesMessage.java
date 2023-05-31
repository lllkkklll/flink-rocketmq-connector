package kai.lu.rocketmq.flink.source.reader.deserializer;

import java.util.HashMap;
import java.util.Map;

/** Message contains byte array. */
public class BytesMessage {

    private byte[] data;

    private Map<String, String> properties = new HashMap<>();

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> props) {
        this.properties = props;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }
}
