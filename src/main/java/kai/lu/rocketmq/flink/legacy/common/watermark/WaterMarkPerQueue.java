package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.rocketmq.common.message.MessageQueue;

import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WaterMarkPerQueue {

    private ConcurrentMap<MessageQueue, Long> maxEventTimeTable;

    private long maxOutOfOrderness = 5000L; // 5 seconds

    public WaterMarkPerQueue() {}

    public WaterMarkPerQueue(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
        maxEventTimeTable = new ConcurrentHashMap<>();
    }

    public void extractTimestamp(MessageQueue mq, long timestamp) {
        long maxEventTime = maxEventTimeTable.getOrDefault(mq, maxOutOfOrderness);
        maxEventTimeTable.put(mq, Math.max(maxEventTime, timestamp));
    }

    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        long minTimestamp = maxOutOfOrderness;
        for (Map.Entry<MessageQueue, Long> entry : maxEventTimeTable.entrySet()) {
            minTimestamp = Math.min(minTimestamp, entry.getValue());
        }
        return new Watermark(minTimestamp - maxOutOfOrderness);
    }

    @Override
    public String toString() {
        return "WaterMarkPerQueue{"
                + "maxEventTimeTable="
                + maxEventTimeTable
                + ", maxOutOfOrderness="
                + maxOutOfOrderness
                + '}';
    }
}
