package kai.lu.rocketmq.flink.sink;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RocketMQWriterState {

    private final String transactionalIdPrefix;

    RocketMQWriterState(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMQWriterState that = (RocketMQWriterState) o;
        return transactionalIdPrefix.equals(that.transactionalIdPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionalIdPrefix);
    }

    @Override
    public String toString() {
        return "KafkaWriterState{"
                + ", transactionalIdPrefix='"
                + transactionalIdPrefix
                + '\''
                + '}';
    }
}
