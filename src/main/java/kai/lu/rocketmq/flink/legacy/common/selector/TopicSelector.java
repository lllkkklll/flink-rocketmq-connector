package kai.lu.rocketmq.flink.legacy.common.selector;

import java.io.Serializable;

public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);
}
