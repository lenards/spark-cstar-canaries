package net.lenards;

import net.lenards.types.EventRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Verify that you can read from Cassandra with Java using EventRecord Bean.
 */
public class SparkCstarRead {

    public static void main(String[] ndy) {
        SparkConf conf = new SparkConf(true)
                        .set("spark.cassandra.connection.host", "127.0.0.1")
                        .setMaster("local[3]")
                        .setAppName(SparkCstarRead.class.toString());

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<EventRecord> rdd = javaFunctions(sc)
                                    .cassandraTable("canary", "eventrecord",
                                        mapRowTo(EventRecord.class));

        System.out.println("\n\n" + rdd.toArray().toString() + "\n\n");

        sc.stop();
    }
}