package kai.lu.rocketmq.flink.legacy.common.selector;

public class DefaultTopicSelector<T> implements TopicSelector<T> {
    private final String topicName;
    private final String tagName;

    public DefaultTopicSelector(final String topicName, final String tagName) {
        this.topicName = topicName;
        this.tagName = tagName;
    }

    public DefaultTopicSelector(final String topicName) {
        this(topicName, "");
    }

    @Override
    public String getTopic(T tuple) {
        return topicName;
    }

    @Override
    public String getTag(T tuple) {
        return tagName;
    }
}
