package net.lenards;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.japi.CassandraRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SparkCanarySimple implements Serializable {

    private static void verifyArgs(String[] args) {
        if (args.length != 2) {
            System.err.println("Arguments required");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        verifyArgs(args);

        SparkConf conf = new SparkConf()
            .setAppName("Spark Canary (Simple) - Test")
            .setMaster(args[0])
            .set("spark.cassandra.connection.host", args[1]);

        JavaSparkContext jsc = new JavaSparkContext(conf);
        System.out.println("it's a start, yo!");
        CassandraJavaRDD<CassandraRow> rdd1 = javaFunctions(jsc)
            .cassandraTable("canary_ks", "kv")
            .select("key", "value");
        long kvs = rdd1.count();
        System.out.println("Count: " + kvs);

        jsc.stop();
    }
}
