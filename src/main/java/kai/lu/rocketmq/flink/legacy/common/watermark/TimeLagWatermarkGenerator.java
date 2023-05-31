package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.rocketmq.common.message.MessageExt;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * This generator generates watermarks that are lagging behind processing time by a certain amount.
 * It assumes that elements arrive in Flink after at most a certain time.
 */
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<MessageExt> {

    private long maxTimeLag = 5000; // 5 seconds

    TimeLagWatermarkGenerator() {}

    TimeLagWatermarkGenerator(long maxTimeLag) {
        this.maxTimeLag = maxTimeLag;
    }

    @Override
    public long extractTimestamp(MessageExt element, long previousElementTimestamp) {
        return element.getBornTimestamp();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }

    @Override
    public String toString() {
        return "TimeLagWatermarkGenerator{" + "maxTimeLag=" + maxTimeLag + '}';
    }
}
