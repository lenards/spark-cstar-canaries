package net.lenards.kinesis;

import java.io.Serializable;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.util.Clock;
import org.apache.spark.streaming.util.ManualClock;
import org.apache.spark.streaming.util.SystemClock;

public class KinesisCheckpointState implements Serializable {
    private Duration checkpointInterval;
    private ManualClock checkpointClock;

    public KinesisCheckpointState(Duration interval) {
        this(interval, new SystemClock());
    }

    public KinesisCheckpointState(Duration interval, Clock current) {
        this.checkpointInterval = interval;
        this.checkpointClock = new ManualClock();
        checkpointClock.setTime(current.currentTime() +
                                checkpointInterval.milliseconds());
    }

    public boolean shouldCheckpoint() {
        return (new SystemClock()).currentTime() > this.checkpointClock.currentTime();
    }

    public void advanceCheckpoint() {
        this.checkpointClock.addToTime(checkpointInterval.milliseconds());
    }
}