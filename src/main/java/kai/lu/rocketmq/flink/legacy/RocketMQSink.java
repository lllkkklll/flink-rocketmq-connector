package kai.lu.rocketmq.flink.legacy;

import kai.lu.rocketmq.flink.legacy.common.util.MetricUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * The RocketMQSink provides at-least-once reliability guarantees when checkpoints are enabled and
 * batchFlushOnCheckpoint(true) is set. Otherwise, the sink reliability guarantees depends on
 * rocketmq producer's retry policy.
 */
public class RocketMQSink extends RichSinkFunction<Message> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);
    private final Properties props;
    private transient DefaultMQProducer producer;
    private boolean batchFlushOnCheckpoint; // false by default
    private List<Message> batchList;

    private Meter sinkInTps;
    private Meter outTps;
    private Meter outBps;
    private MetricUtils.LatencyGauge latencyGauge;

    public RocketMQSink(Properties props) {
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Validate.notEmpty(props, "Producer properties can not be empty");

        // with authentication hook
        producer = new DefaultMQProducer(RocketMQConfig.buildAclRPCHook(props));
        producer.setInstanceName(
                getRuntimeContext().getIndexOfThisSubtask() + "_" + UUID.randomUUID());

        RocketMQConfig.buildProducerConfigs(props, producer);

        batchList = new LinkedList<>();

        if (batchFlushOnCheckpoint &&
                !((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.info("Flushing on checkpoint is enabled, but checkpointing is not enabled. " +
                    "Disabling flushing.");
            batchFlushOnCheckpoint = false;
        }

        try {
            producer.start();
        } catch (MQClientException e) {
            LOG.error("Flink sink init failed, due to the producer cannot be initialized.");
            throw new RuntimeException(e);
        }
        sinkInTps = MetricUtils.registerSinkInTps(getRuntimeContext());
        outTps = MetricUtils.registerOutTps(getRuntimeContext());
        outBps = MetricUtils.registerOutBps(getRuntimeContext());
        latencyGauge = MetricUtils.registerOutLatency(getRuntimeContext());
    }

    @Override
    public void invoke(Message input, Context context) throws Exception {
        sinkInTps.markEvent();

        if (batchFlushOnCheckpoint) {
            batchList.add(input);
            int batchSize = 32;
            if (batchList.size() >= batchSize) {
                flushSync();
            }
            return;
        }

        long timeStartWriting = System.currentTimeMillis();
        try {
            SendResult result = producer.send(input);
            LOG.debug("Sync send message result: {}", result);
            if (result.getSendStatus() != SendStatus.SEND_OK) {
                throw new RemotingException(result.toString());
            }
            long end = System.currentTimeMillis();
            latencyGauge.report(end - timeStartWriting, 1);
            outTps.markEvent();
            outBps.markEvent(input.getBody().length);
        } catch (Exception e) {
            LOG.error("Sync send message exception: ", e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                flushSync();
            } catch (Exception e) {
                LOG.error("FlushSync failure!", e);
            }
            // make sure producer can be shutdown, thus current producerGroup will be unregistered
            producer.shutdown();
        }
    }

    private void flushSync() throws Exception {
        if (batchFlushOnCheckpoint) {
            synchronized (batchList) {
                if (batchList.size() > 0) {
                    producer.send(batchList);
                    batchList.clear();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushSync();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to do
    }
}
