package net.lenards.kinesis.types;

import net.lenards.kinesis.KinesisCheckpointState;

import org.apache.spark.streaming.Duration;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class EventRecordProcessorFactory implements IRecordProcessorFactory {
    private JKinesisReceiver receiver;
    private String workerId;
    private Duration checkpointInterval;

    public EventRecordProcessorFactory(JKinesisReceiver receiver,
                                       String workerId, Duration chkpt) {
        this.receiver = receiver;
        this.workerId = workerId;
        this.checkpointInterval = chkpt;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new EventRecordProcessor(receiver, workerId,
                                new KinesisCheckpointState(checkpointInterval));
    }
}