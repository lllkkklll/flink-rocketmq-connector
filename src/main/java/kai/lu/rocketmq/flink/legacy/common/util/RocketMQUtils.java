package kai.lu.rocketmq.flink.legacy.common.util;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.common.message.MessageQueue;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public final class RocketMQUtils {

    public static int getInteger(Properties props, String key, int defaultValue) {
        return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static long getLong(Properties props, String key, long defaultValue) {
        return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static AccessChannel getAccessChannel(
            Properties props, String key, AccessChannel defaultValue) {
        return AccessChannel.valueOf(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static String getInstanceName(String... args) {
        if (null != args && args.length > 0) {
            return String.join("_", args);
        }
        return ManagementFactory.getRuntimeMXBean().getName() + "_" + System.nanoTime();
    }

    /**
     * Average Hashing queue algorithm Refer:
     * org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely
     */
    public static List<MessageQueue> allocate(
            Collection<MessageQueue> mqSet, int numberOfParallelTasks, int indexOfThisTask) {

        ArrayList<MessageQueue> mqAll = new ArrayList<>(mqSet);
        Collections.sort(mqAll);
        List<MessageQueue> result = new ArrayList<>();
        int mod = mqAll.size() % numberOfParallelTasks;
        int averageSize = mqAll.size() <= numberOfParallelTasks ?
                1 : (mod > 0 && indexOfThisTask < mod ? mqAll.size() / numberOfParallelTasks + 1
                : mqAll.size() / numberOfParallelTasks);
        int startIndex = (mod > 0 && indexOfThisTask < mod) ? indexOfThisTask * averageSize
                : indexOfThisTask * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }
}
