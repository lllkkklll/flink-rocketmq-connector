package kai.lu.rocketmq.flink.source.reader;

import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplit;
import kai.lu.rocketmq.flink.source.split.RocketMQPartitionSplitState;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

/** The source reader for RocketMQ partitions. */
public class RocketMQSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<
                Tuple3<T, Long, Long>, T, RocketMQPartitionSplit, RocketMQPartitionSplitState> {

    public RocketMQSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
            Supplier<SplitReader<Tuple3<T, Long, Long>, RocketMQPartitionSplit>> splitReaderSupplier,
            RecordEmitter<Tuple3<T, Long, Long>, T, RocketMQPartitionSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {

        super(elementsQueue, splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, RocketMQPartitionSplitState> map) {}

    @Override
    protected RocketMQPartitionSplitState initializedState(RocketMQPartitionSplit partitionSplit) {
        return new RocketMQPartitionSplitState(partitionSplit);
    }

    @Override
    protected RocketMQPartitionSplit toSplitType(String splitId, RocketMQPartitionSplitState splitState) {
        return splitState.toRocketMQPartitionSplit();
    }
}
