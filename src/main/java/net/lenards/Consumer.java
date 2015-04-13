package net.lenards;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

// KinesisCheckpointState related
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.util.Clock;
import org.apache.spark.streaming.util.ManualClock;
import org.apache.spark.streaming.util.SystemClock;

class SerializableDefaultAWSCredentialsProviderChain
    extends DefaultAWSCredentialsProviderChain
    implements Serializable {

}

class KinesisCheckpointState implements Serializable {
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

public class Consumer implements Serializable {
    private static final String APP = "StockTradesProcessor";
    private static final String VERSION = "0.0.1";

    public static ClientConfiguration CLIENT_CONF;
    public static AWSCredentialsProvider CREDS;
    public static KinesisClientLibConfiguration KCL_CONFIG;

    static {
        ClientConfiguration config = new ClientConfiguration();
        config.setUserAgent(String.format("%s %s/%s",
                            ClientConfiguration.DEFAULT_USER_AGENT,
                            APP, VERSION));
        CLIENT_CONF = config;
    }

    private static class JKinesisReceiver extends Receiver<byte[]> implements Serializable {
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

            this.recordProcessorFactory = new IRecordProcessorFactory() {
                @Override
                public IRecordProcessor createProcessor() {
                    return new EventRecordProcessor(JKinesisReceiver.this, workerId,
                                new KinesisCheckpointState(checkpointInterval));
                }
            };

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

    private static class EventRecordProcessor implements IRecordProcessor, Serializable {
        public static final long DEFAULT_INTERVAL_IN_MS = 60000L;

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
            handleRecords(records);
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
                this.receiver.store(r.getData().array());
                System.out.println(String.format("%s: %s", r.getPartitionKey(),
                    r.getData().array()));
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

    public static void verify(String[] args) {
        System.out.println(Arrays.asList(args));
        if (!(args.length == 4)) {
            System.out.println("Usage: \n\tConsumer " +
                "<app-name> <stream-name> <endpoint-url> <aws-region>");
            System.exit(1);
        }
    }

    public static AWSCredentialsProvider getCredsProvider() throws Exception {
        String msg = "Cannot load AWS credentials, no 'default' profile available.";

        try {
            AWSCredentialsProvider provider =
                new ProfileCredentialsProvider("default");
            return provider;
            //return new SerializableDefaultAWSCredentialsProviderChain();
        } catch (Exception e) {
            throw new AmazonClientException(msg, e);
        }
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ex) {
            return "localhost";
        }
    }

    public static void main(String[] args) throws Exception {
        verify(args);
        String appName = args[0];
        String stream = args[1];
        String endptUrl = args[2];
        Region region = RegionUtils.getRegion(args[3]);

        CREDS = getCredsProvider();

        String workerId = getHostname() + ":" + String.valueOf(UUID.randomUUID());

        KinesisClientLibConfiguration kclConfig
                    = new KinesisClientLibConfiguration(appName, stream, CREDS,
                                                       workerId)
                            .withKinesisEndpoint(endptUrl)
                            .withRegionName(region.getName())
                            .withCommonClientConfig(Consumer.CLIENT_CONF)
                            .withInitialPositionInStream(InitialPositionInStream.LATEST)
                            .withTaskBackoffTimeMillis(500);


        SparkConf conf = new SparkConf(true)
                        .set("spark.cassandra.connection.host", "127.0.0.1")
                        .setMaster("local[3]")
                        .setAppName(appName);

        Duration batchInterval = new Duration(EventRecordProcessor.DEFAULT_INTERVAL_IN_MS);

        Duration checkpointInterval = batchInterval;

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, batchInterval);

        JKinesisReceiver receiver = new JKinesisReceiver(kclConfig, workerId,
                                                         checkpointInterval,
                                                         InitialPositionInStream.LATEST);

        JavaDStream<byte[]> dstream = jssc.receiverStream(receiver);

        dstream.print();

        // gracefully stop the Spark Streaming example
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Inside Add Shutdown Hook");
                jssc.stop(true, true);
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}