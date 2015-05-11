package net.lenards;

import net.lenards.kinesis.KinesisCheckpointState;
import net.lenards.kinesis.types.*;
import net.lenards.types.EventRecord;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


class EventRecordMapFunc implements Function<String, EventRecord>, Serializable {
    @Override
    public EventRecord call(String record) {
        // validation would be a good idea ...
        String[] pieces = record.split("\\|");
        if (pieces.length == 2) {
            String[] keys = pieces[0].split(":");
            String[] fields = pieces[1].split(";");
            long ts = DatatypeConverter.parseDateTime(fields[0]).getTimeInMillis();
            return new EventRecord(keys[0], keys[1], ts, fields[1],
                                   Integer.valueOf(fields[2]));
        }
        // if the format is off, just give by an empty object;
        return new EventRecord();
    }
}


public class Consumer implements Serializable {

    private String appName;
    private String streamName;
    private String endpointUrl;
    private String regionName;

    private Duration checkpointInterval;

    private SparkConf conf;

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
    }

    public void start() {
        final JavaStreamingContext context = new JavaStreamingContext(conf, checkpointInterval);

        // for graceful shutdown of the application ...
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down streaming app...");
                context.stop(true, true);
                System.out.println("Shutdown of streaming app complete.");
            }
        });

        JKinesisReceiver receiver = new JKinesisReceiver(appName, streamName,
                                                         endpointUrl, regionName,
                                                         checkpointInterval,
                                                         InitialPositionInStream.LATEST);

        JavaDStream<String> dstream = context.receiverStream(receiver);

        JavaDStream<EventRecord> recs = dstream.map(new EventRecordMapFunc());

        recs.print();

        // persist to DStream to Cassandra
        javaFunctions(recs)
            .writerBuilder("canary", "eventrecord", mapToRow(EventRecord.class))
            .saveToCassandra();


        System.out.println("w00 w00!");

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
        System.out.println(
            Arrays.asList(
                ((URLClassLoader) (Thread.currentThread().getContextClassLoader())).getURLs()
            )
        );
/*
        ClassLoader cloader = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cloader).getURLs();
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
 */
        verify(args);
        Consumer c = new Consumer(args[0], args[1], args[2], args[3]);
        c.start();
    }
}