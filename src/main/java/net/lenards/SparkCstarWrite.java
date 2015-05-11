package net.lenards;

import net.lenards.types.EventRecord;

import java.util.Arrays;
import java.util.Date;
import javax.xml.bind.DatatypeConverter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Verify that you can write from Cassandra with Java using EventRecord Bean.
 */
public class SparkCstarWrite {

    public static void main(String[] ndy) {
        SparkConf conf = new SparkConf(true)
                        .set("spark.cassandra.connection.host", "127.0.0.1")
                        .setMaster("local[3]")
                        .setAppName(SparkCstarWrite.class.toString());

        JavaSparkContext sc = new JavaSparkContext(conf);

        long currentTime = System.currentTimeMillis();
        JavaRDD<EventRecord> rdd = sc.parallelize(Arrays.asList(new EventRecord[] {
            new EventRecord("bootstrap", "600", currentTime, "test", 1),
            new EventRecord("bootstrap", "600", currentTime+1, "test", 2),
            new EventRecord("bootstrap", "600", currentTime+2, "test", 3),
            new EventRecord("bootstrap", "600", currentTime+3, "test", 4)
        }));


        javaFunctions(rdd)
            .writerBuilder("canary", "eventrecord", mapToRow(EventRecord.class))
            .saveToCassandra();

        sc.stop();
    }
}