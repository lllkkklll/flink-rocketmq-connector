package kai.lu.rocketmq.flink.sink;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * This class holds the necessary information to construct a new {@link FlinkRocketMQInternalProducer}
 * to commit transactions in {@link RocketMQCommitter}.
 */
public class RocketMQCommittable {

    private final long producerId;
    private final short epoch;
    private final String transactionalId;
    @Nullable
    private Recyclable<? extends FlinkRocketMQInternalProducer<?, ?>> producer;

    public RocketMQCommittable(
            long producerId,
            short epoch,
            String transactionalId,
            @Nullable Recyclable<? extends FlinkRocketMQInternalProducer<?, ?>> producer) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.transactionalId = transactionalId;
        this.producer = producer;
    }

    public static <K, V> RocketMQCommittable of(
            FlinkRocketMQInternalProducer<K, V> producer,
            Consumer<FlinkRocketMQInternalProducer<K, V>> recycler) {
        return new RocketMQCommittable(
                producer.getProducerId(),
                producer.getEpoch(),
                producer.getTransactionalId(),
                new Recyclable<>(producer, recycler));
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public Optional<Recyclable<? extends FlinkRocketMQInternalProducer<?, ?>>> getProducer() {
        return Optional.ofNullable(producer);
    }

    @Override
    public String toString() {
        return "KafkaCommittable{"
                + "producerId="
                + producerId
                + ", epoch="
                + epoch
                + ", transactionalId="
                + transactionalId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMQCommittable that = (RocketMQCommittable) o;
        return producerId == that.producerId
                && epoch == that.epoch
                && transactionalId.equals(that.transactionalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, epoch, transactionalId);
    }
}
