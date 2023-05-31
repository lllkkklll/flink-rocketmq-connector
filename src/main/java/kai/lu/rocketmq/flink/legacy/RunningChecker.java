package kai.lu.rocketmq.flink.legacy;

import java.io.Serializable;

public class RunningChecker implements Serializable {

    private volatile boolean isRunning = false;

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
