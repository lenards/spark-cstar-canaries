package net.lenards;

import net.lenards.kinesis.KinesisCheckpointState;
import net.lenards.kinesis.types.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

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


public class Consumer implements Serializable {

    private String appName;
    private String streamName;
    private String endpointUrl;
    private String regionName;

    private Duration checkpointInterval;

    private SparkConf conf;
    private JavaStreamingContext context;

    public Consumer(String appName, String streamName, String endpointUrl,
                    String regionName) {
        this.appName = appName;
        this.streamName = streamName;
        this.endpointUrl = endpointUrl;
        this.regionName = regionName;
        this.checkpointInterval = new Duration(EventRecordProcessor.DEFAULT_INTERVAL_IN_MS);
        init();
    }

    private void init() {
        this.conf = new SparkConf(true)
                        .set("spark.cassandra.connection.host", "127.0.0.1")
                        .setMaster("local[3]")
                        .setAppName(this.appName);

        context = new JavaStreamingContext(conf, checkpointInterval);
    }

    public void start() {
        JKinesisReceiver receiver = new JKinesisReceiver(appName, streamName,
                                                         endpointUrl, regionName,
                                                         checkpointInterval,
                                                         InitialPositionInStream.LATEST);

        JavaDStream<byte[]> dstream = context.receiverStream(receiver);

        dstream.print();

        // gracefully stop the Spark Streaming example
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Inside Add Shutdown Hook");
                context.stop(true, true);
            }
        });

        context.start();
        context.awaitTermination();

    }

    public static void verify(String[] args) {
        System.out.println(Arrays.asList(args));
        if (!(args.length == 4)) {
            System.out.println("Usage: \n\tConsumer " +
                "<app-name> <stream-name> <endpoint-url> <aws-region>");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        verify(args);
        String appName = args[0];
        String streamName = args[1];
        String endpointUrl = args[2];
        String regionName = args[3];
        Consumer c = new Consumer(appName, streamName, endpointUrl, regionName);
        c.start();
    }
}