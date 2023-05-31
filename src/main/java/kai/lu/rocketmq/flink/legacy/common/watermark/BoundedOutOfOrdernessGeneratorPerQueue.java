package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.rocketmq.common.message.MessageExt;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BoundedOutOfOrdernessGeneratorPerQueue implements AssignerWithPeriodicWatermarks<MessageExt> {

    private Map<String, Long> maxEventTimeTable;
    private long maxOutOfOrderness = 5000L; // 5 seconds

    public BoundedOutOfOrdernessGeneratorPerQueue() {}

    public BoundedOutOfOrdernessGeneratorPerQueue(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        maxEventTimeTable = new ConcurrentHashMap<>();
    }

    @Override
    public long extractTimestamp(MessageExt element, long previousElementTimestamp) {
        String key = element.getBrokerName() + "_" + element.getQueueId();
        Long maxEventTime = maxEventTimeTable.getOrDefault(key, maxOutOfOrderness);
        long timestamp = element.getBornTimestamp();
        maxEventTimeTable.put(key, Math.max(maxEventTime, timestamp));
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        long minTimestamp = 0L;
        for (Map.Entry<String, Long> entry : maxEventTimeTable.entrySet()) {
            minTimestamp = Math.min(minTimestamp, entry.getValue());
        }
        return new Watermark(minTimestamp - maxOutOfOrderness);
    }

    @Override
    public String toString() {
        return "BoundedOutOfOrdernessGeneratorPerQueue{"
                + "maxEventTimeTable="
                + maxEventTimeTable
                + ", maxOutOfOrderness="
                + maxOutOfOrderness
                + '}';
    }
}
