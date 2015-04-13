package net.lenards;

import net.lenards.kinesis.KinesisCheckpointState;
import net.lenards.kinesis.types.*;

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

class SerializableDefaultAWSCredentialsProviderChain
    extends DefaultAWSCredentialsProviderChain
    implements Serializable {

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