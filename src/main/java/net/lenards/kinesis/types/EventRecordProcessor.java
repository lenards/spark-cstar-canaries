package net.lenards.kinesis.types;

import java.io.Serializable;
import java.util.List;

import net.lenards.kinesis.KinesisCheckpointState;
import net.lenards.kinesis.types.JKinesisReceiver;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class EventRecordProcessor implements IRecordProcessor, Serializable {
    public static final long DEFAULT_INTERVAL_IN_MS = 10000L;

    private String shardId;
    private String workerId;
    private JKinesisReceiver receiver;
    private KinesisCheckpointState checkpointState;

    public EventRecordProcessor(JKinesisReceiver receiver, String workerId,
                                KinesisCheckpointState checkpointState) {
        this.workerId = workerId;
        this.receiver = receiver;
        this.checkpointState = checkpointState;
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records,
                               IRecordProcessorCheckpointer checkpointer) {
        if (!receiver.isStopped()) {
            // this processor works in tandem with the Spark Streaming receiver
            handleRecords(records);
            checkpointIfNeeded(checkpointer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        // Important to checkpoint after reaching end of shard,
        // so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            performCheckpoint(checkpointer);
        }
    }

    private void handleRecords(List<Record> records) {
        for (Record r : records) {
            // NOTE:
            // this is how we're passing processed data back to Spark Streaming
            this.receiver.store(r.getPartitionKey() + "|" +
                                new String(r.getData().array()));
        }
    }

    private void checkpointIfNeeded(IRecordProcessorCheckpointer checkpointer) {
        if (checkpointState.shouldCheckpoint()) {
            performCheckpoint(checkpointer);
            checkpointState.advanceCheckpoint();
        }
    }

    private void performCheckpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (Exception ex) {
            System.out.println("Sky is falling! Why? " + ex);
        }
    }
}
