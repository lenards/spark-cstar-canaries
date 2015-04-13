package net.lenards.kinesis.types;

import java.io.Serializable;

import net.lenards.kinesis.KinesisCheckpointState;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.receiver.Receiver;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class JKinesisReceiver extends Receiver<byte[]> implements Serializable {
    private KinesisClientLibConfiguration kclConfig;
    private String workerId;
    private Duration checkpointInterval;
    private InitialPositionInStream initialPosition;
    private StorageLevel storageLevel;
    private IRecordProcessorFactory recordProcessorFactory;
    private Worker worker;

    public JKinesisReceiver(KinesisClientLibConfiguration kclConfig,
                            String workerId,
                            Duration checkpoint,
                            InitialPositionInStream position) {
        super(StorageLevel.MEMORY_ONLY());
        this.kclConfig = kclConfig;
        this.workerId = workerId;
        this.checkpointInterval = checkpoint;
        this.initialPosition = position;
        this.storageLevel = StorageLevel.MEMORY_ONLY();
    }


    @Override
    public void onStart() {
        this.recordProcessorFactory = new EventRecordProcessorFactory(this,
                                                                      workerId,
                                                                      checkpointInterval);

        this.worker = new Worker(this.recordProcessorFactory, this.kclConfig);
        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    @Override
    public void onStop() {
        this.worker.shutdown();
        this.workerId = null;
        this.recordProcessorFactory = null;
        this.worker = null;
    }
}
