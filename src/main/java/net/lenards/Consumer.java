package net.lenards;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class Consumer {
    private static final String APP = "StockTradesProcessor";
    private static final String VERSION = "0.0.1";

    private static ClientConfiguration CLIENT_CONF;

    static {
        ClientConfiguration config = new ClientConfiguration();
        config.setUserAgent(String.format("%s %s/%s",
                            ClientConfiguration.DEFAULT_USER_AGENT,
                            APP, VERSION));
        CLIENT_CONF = config;
    }

    private static class EventRecordProcessor implements IRecordProcessor {
        public static final long DEFAULT_INTERVAL_IN_MS = 60000L;

        private String shardId;
        private long nextOutput;
        private long nextCheckpoint;

        @Override
        public void initialize(String shardId) {
            this.shardId = shardId;
            this.nextOutput = System.currentTimeMillis() + DEFAULT_INTERVAL_IN_MS;
            this.nextCheckpoint = System.currentTimeMillis() + DEFAULT_INTERVAL_IN_MS;
        }

        @Override
        public void processRecords(List<Record> records,
                                   IRecordProcessorCheckpointer checkpointer) {
            handleRecords(records);
            outputIfNeeded();
            checkpointIfNeeded(checkpointer);
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
                System.out.println(String.format("%s: %s", r.getPartitionKey(),
                    r.getData().array()));
            }
        }

        private void outputIfNeeded() {
            long currentTime = System.currentTimeMillis();
            if (currentTime > this.nextOutput) {
                System.out.println("... we're still alive.");
                this.nextOutput = currentTime + DEFAULT_INTERVAL_IN_MS;
            }
        }

        private void checkpointIfNeeded(IRecordProcessorCheckpointer checkpointer) {
            long currentTime = System.currentTimeMillis();
            if (currentTime > this.nextCheckpoint) {
                performCheckpoint(checkpointer);
                this.nextCheckpoint = currentTime + DEFAULT_INTERVAL_IN_MS;
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

    public static void verify(String[] args) {
        System.out.println(Arrays.asList(args));
        if (!(args.length == 3)) {
            System.out.println("Usage: \n\tConsumer " +
                "<app-name> <stream-name> <aws-region>");
            System.exit(1);
        }
    }

    public static AWSCredentialsProvider getCredsProvider() throws Exception {
        String msg = "Cannot load AWS credentials, no 'default' profile available.";

        try {
            AWSCredentialsProvider provider =
                new ProfileCredentialsProvider("default");
            return provider;
        } catch (Exception e) {
            throw new AmazonClientException(msg, e);
        }
    }

    public static void process(KinesisClientLibConfiguration kclConfig) {
        IRecordProcessorFactory recordProcessorFactory = new IRecordProcessorFactory() {
            @Override
            public IRecordProcessor createProcessor() {
                return new EventRecordProcessor();
            }
        };

        // Create the KCL worker with the stock trade record processor factory
        Worker worker = new Worker(recordProcessorFactory, kclConfig);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    public static void main(String[] args) throws Exception {
        verify(args);
        String appName = args[0];
        String stream = args[1];
        Region region = RegionUtils.getRegion(args[2]);

        AWSCredentialsProvider credentials = getCredsProvider();

        String workerId = String.valueOf(UUID.randomUUID());
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(appName, stream, credentials, workerId)
                    .withRegionName(region.getName())
                    .withCommonClientConfig(CLIENT_CONF);

        process(kclConfig);
    }
}