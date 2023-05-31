package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.flink.streaming.api.watermark.Watermark;

public class WaterMarkForAll {

    private long maxOutOfOrderness = 5000L; // 5 seconds

    private long maxTimestamp = 0L;

    public WaterMarkForAll() {}

    public WaterMarkForAll(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    public void extractTimestamp(long timestamp) {
        maxTimestamp = Math.max(timestamp, maxTimestamp);
    }

    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp - maxOutOfOrderness);
    }
}
