package kai.lu.rocketmq.flink.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A row data wrapper class that wraps a {@link RocketMQDeserializationSchema} to deserialize {@link
 * MessageExt}.
 */
public class DynamicRocketMQDeserializationSchema
        implements RocketMQDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> deserializationSchema;

    private final TypeInformation<RowData> producedTypeInfo;

    public DynamicRocketMQDeserializationSchema(
            DeserializationSchema<RowData> deserializationSchema,
            TypeInformation<RowData> producedTypeInfo) {

        this.deserializationSchema = deserializationSchema;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        deserializationSchema.open(context);
    }

    @Override
    public void deserialize(byte[] input, Collector<RowData> collector) throws IOException {
        if (null == input) {
            collector.collect(null);
        } else {
            deserializationSchema.deserialize(input, collector);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    public interface MetadataConverter extends Serializable {
        Object read(MessageExt message);
    }

    // --------------------------------------------------------------------------------------------

    private static final class BufferingCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<RowData> buffer = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            buffer.add(record);
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
