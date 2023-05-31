package kai.lu.rocketmq.flink.legacy.function;

import org.apache.commons.lang3.Validate;
import org.apache.rocketmq.common.message.Message;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SinkMapFunction extends ProcessFunction<Tuple2<String, String>, Message> {

    private String topic;

    private String tag;

    public SinkMapFunction() {}

    public SinkMapFunction(String topic, String tag) {
        this.topic = topic;
        this.tag = tag;
    }

    @Override
    public void processElement(Tuple2<String, String> tuple, Context ctx, Collector<Message> out) throws Exception {
        Validate.notNull(topic, "the message topic is null");
        Validate.notNull(tuple.f1.getBytes(), "the message body is null");

        Message message = new Message(topic, tag, tuple.f0, tuple.f1.getBytes());
        out.collect(message);
    }
}
