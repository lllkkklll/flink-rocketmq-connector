package kai.lu.rocketmq.flink.legacy.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SourceMapFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, String>> {

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {

        out.collect(new Tuple2<>(value.f0, value.f1));
    }
}
