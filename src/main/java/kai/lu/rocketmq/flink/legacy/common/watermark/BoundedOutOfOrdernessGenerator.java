package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.rocketmq.common.message.MessageExt;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<MessageExt> {

    private long maxOutOfOrderness = 5000; // 5 seconds

    private long currentMaxTimestamp;

    public BoundedOutOfOrdernessGenerator() {}

    public BoundedOutOfOrdernessGenerator(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    @Override
    public long extractTimestamp(MessageExt element, long previousElementTimestamp) {
        long timestamp = element.getBornTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public String toString() {
        return "BoundedOutOfOrdernessGenerator{"
                + "maxOutOfOrderness="
                + maxOutOfOrderness
                + ", currentMaxTimestamp="
                + currentMaxTimestamp
                + '}';
    }
}
