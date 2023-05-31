package kai.lu.rocketmq.flink.legacy.common.watermark;

import org.apache.rocketmq.common.message.MessageExt;
import kai.lu.rocketmq.flink.legacy.RocketMQConfig;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * With Punctuated Watermarks To generate watermarks whenever a certain event indicates that a new
 * watermark might be generated, use AssignerWithPunctuatedWatermarks. For this class Flink will
 * first call the extractTimestamp(...) method to assign the element a timestamp, and then
 * immediately call the checkAndGetNextWatermark(...) method on that element.
 *
 * <p>The checkAndGetNextWatermark(...) method is passed the timestamp that was assigned in the
 * extractTimestamp(...) method, and can decide whether it wants to generate a watermark. Whenever
 * the checkAndGetNextWatermark(...) method returns a non-null watermark, and that watermark is
 * larger than the latest previous watermark, that new watermark will be emitted.
 */
public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<MessageExt> {

    @Override
    public long extractTimestamp(MessageExt element, long previousElementTimestamp) {
        return element.getBornTimestamp();
    }

    @Override
    public Watermark checkAndGetNextWatermark(MessageExt lastElement, long extractedTimestamp) {
        String lastValue = lastElement.getProperty(RocketMQConfig.WATERMARK);
        return lastValue != null ? new Watermark(extractedTimestamp) : null;
    }
}
