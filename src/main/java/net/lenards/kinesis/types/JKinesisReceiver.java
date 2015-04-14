package net.lenards.kinesis.types;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.UUID;

import net.lenards.kinesis.KinesisCheckpointState;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.receiver.Receiver;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class JKinesisReceiver extends Receiver<byte[]> implements Serializable {
    private static final String APP = "StockTradesProcessor";
    private static final String VERSION = "0.0.1";

    private static ClientConfiguration CLIENT_CONF;

    private KinesisClientLibConfiguration kclConfig;
    private String workerId;
    private Duration checkpointInterval;
    private InitialPositionInStream initialPosition;
    private StorageLevel storageLevel;
    private IRecordProcessorFactory recordProcessorFactory;
    private Worker worker;

    static {
        ClientConfiguration config = new ClientConfiguration();
        config.setUserAgent(String.format("%s %s/%s",
                            ClientConfiguration.DEFAULT_USER_AGENT,
                            APP, VERSION));
        CLIENT_CONF = config;
    }

    public JKinesisReceiver(String applicationName, String streamName,
                            String endpointUrl, String regionName,
                            Duration checkpoint, InitialPositionInStream position) {
        super(StorageLevel.MEMORY_ONLY());

        this.storageLevel = StorageLevel.MEMORY_ONLY();
        this.workerId = getHostname() + ":" + String.valueOf(UUID.randomUUID());
        this.checkpointInterval = checkpoint;
        this.initialPosition = position;

        Region region = RegionUtils.getRegion(regionName);

        try {
            this.kclConfig = new KinesisClientLibConfiguration(applicationName, streamName,
                                                      getCredsProvider(),
                                                      workerId)
                            .withKinesisEndpoint(endpointUrl)
                            .withRegionName(region.getName())
                            .withCommonClientConfig(CLIENT_CONF)
                            .withInitialPositionInStream(InitialPositionInStream.LATEST)
                            .withTaskBackoffTimeMillis(500);
        } catch (Exception ex) {
            // do absolutely nothing - and feel good about it!
        }
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ex) {
            return "localhost";
        }
    }

    private static AWSCredentialsProvider getCredsProvider() throws Exception {
        String msg = "Cannot load AWS credentials, no 'default' profile available.";

        try {
            //AWSCredentialsProvider provider =
            //    new ProfileCredentialsProvider("default");
            //return provider;
            return new DefaultAWSCredentialsProviderChain();
        } catch (Exception e) {
            throw new AmazonClientException(msg, e);
            //return null;
        }
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
