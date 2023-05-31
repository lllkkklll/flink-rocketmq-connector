package kai.lu.rocketmq.flink.sink;

import kai.lu.rocketmq.flink.sink.serializer.DynamicRocketMQMessageSerializationSchema;
import org.apache.rocketmq.common.message.Message;
import kai.lu.rocketmq.flink.legacy.RocketMQSink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

/** RocketMQRowDataSink helps for writing the converted row data of table to RocketMQ messages. */
public class RocketMQRowDataSink extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final RocketMQSink sink;
    private final DynamicRocketMQMessageSerializationSchema schema;

    public RocketMQRowDataSink(RocketMQSink sink, DynamicRocketMQMessageSerializationSchema schema) {
        this.sink = sink;
        this.schema = schema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        sink.open(configuration);
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        sink.setRuntimeContext(runtimeContext);
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        Message message = schema.serialize(rowData);

        if (message != null) {
            sink.invoke(message, context);
        }
    }

    @Override
    public void close() {
        sink.close();
    }
}
