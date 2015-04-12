package net.lenards;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;


/**
 * Let's write some data to Amazon Kinesis!
 *
 */
public class Producer
{
    private static final String APP = "StockTradesProcessor";
    private static final String VERSION = "0.0.1";

    private static final String[] EVENT_NAMES = {"baz", "bar", "foo", "qux"};

    private static ClientConfiguration CLIENT_CONF;

    static {
        ClientConfiguration config = new ClientConfiguration();
        config.setUserAgent(String.format("%s %s/%s",
                            ClientConfiguration.DEFAULT_USER_AGENT,
                            APP, VERSION));
        CLIENT_CONF = config;
    }

    public static void verify(String[] args) {
        if (!(args.length == 2)) {
            System.out.println("Usage: \n\tApp <stream-name> <aws-region>");
            System.exit(1);
        }
    }

    public static AWSCredentials getCreds() throws Exception {
        String msg = "Cannot load AWS credentials, no 'default' profile available.";

        try {
            AWSCredentialsProvider provider =
                new ProfileCredentialsProvider("default");
            return provider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(msg, e);
        }
    }

    public static void checkStream(DescribeStreamResult result) {
        String statusText = result.getStreamDescription().getStreamStatus();
        String streamName = result.getStreamDescription().getStreamName();
        if (!statusText.equals("ACTIVE")) {
                System.err.println("Inactive Stream: " + streamName);
                System.exit(1);
        } else {
            System.out.println("Stream " + streamName + " is ACTIVE!");
        }
    }

    //  "2014-10-07T12:20:08Z;foo;1"
    public static String getCurrentTimeStr() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(tz);
        return sdf.format(new java.util.Date());
    }

    public static String getRandomEventName() {
        Random rnd = new Random(System.currentTimeMillis());
        int idx = rnd.nextInt(EVENT_NAMES.length);
        return EVENT_NAMES[idx];
    }

    public static String getEventRecord() {
        Random rnd = new Random(System.currentTimeMillis());
        return String.format("%s;%s;%d",
            getCurrentTimeStr(),
            getRandomEventName(),
            rnd.nextInt(10));
    }

    public static void putEventRecord(AmazonKinesis client, String stream) throws Exception {
        String eventRecord = getEventRecord();
        PutRecordRequest put = new PutRecordRequest();
        put.setStreamName(stream);
        put.setPartitionKey("test:650");
        put.setData(ByteBuffer.wrap(eventRecord.getBytes("UTF-8")));

        try {
            PutRecordResult result = client.putRecord(put);
            System.out.println(result);
        } catch (AmazonClientException ex) {
            System.out.println("PutRecord failed.");
        }
    }

    public static void main(String[] args) throws Exception {
        verify(args);
        String stream = args[0];
        Region region = RegionUtils.getRegion(args[1]);

        AWSCredentials credentials = getCreds();
        AmazonKinesis client = new AmazonKinesisClient(credentials, CLIENT_CONF);
        client.setRegion(region);
        checkStream(client.describeStream(stream));

        System.out.println("Let's start putting records!");
        Random rnd = new Random(System.currentTimeMillis());
        for (;;) {
            putEventRecord(client, stream);
            Thread.sleep(rnd.nextInt(500) + 650);
        }
    }
}
