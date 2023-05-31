package kai.lu.rocketmq.flink.source.reader.deserializer;

/** Dirty data process strategy. */
public enum DirtyDataStrategy {
    SKIP,

    SKIP_SILENT,

    CUT,

    PAD,

    NULL,

    EXCEPTION
}
