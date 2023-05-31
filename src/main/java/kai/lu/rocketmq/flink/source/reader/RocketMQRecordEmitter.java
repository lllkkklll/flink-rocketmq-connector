package kai.lu.rocketmq.flink.source.reader;

import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplitState;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** The {@link RecordEmitter} implementation for {@link RocketMQSourceReader}. */
public class RocketMQRecordEmitter<T>
        implements RecordEmitter<Tuple3<T, Long, Long>, T, RocketMQPartitionSplitState> {

    @Override
    public void emitRecord(
            Tuple3<T, Long, Long> element,
            SourceOutput<T> output,
            RocketMQPartitionSplitState splitState) {

        output.collect(element.f0, element.f2);
        splitState.setCurrentOffset(element.f1 + 1);
    }
}
