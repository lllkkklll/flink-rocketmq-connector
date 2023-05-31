package kai.lu.rocketmq.flink.legacy.common.util;

import kai.lu.rocketmq.flink.legacy.RunningChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    private static final long INITIAL_BACKOFF = 200;
    private static final long MAX_BACKOFF = 5000;
    private static final int MAX_ATTEMPTS = 5;

    private RetryUtil() {}

    public static void waitForMs(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T call(Callable<T> callable, String errorMsg) throws RuntimeException {
        return call(callable, errorMsg, null);
    }

    public static <T> T call(Callable<T> callable, String errorMsg, RunningChecker runningChecker)
            throws RuntimeException {
        long backoff = INITIAL_BACKOFF;
        int retries = 0;
        do {
            try {
                return callable.call();
            } catch (Exception ex) {
                if (retries >= MAX_ATTEMPTS) {
                    if (null != runningChecker) {
                        runningChecker.setRunning(false);
                    }
                    throw new RuntimeException(ex);
                }
                log.error("{}, retry {}/{}", errorMsg, retries, MAX_ATTEMPTS, ex);
                retries++;
            }
            waitForMs(backoff);
            backoff = Math.min(backoff * 2, MAX_BACKOFF);
        } while (true);
    }
}
