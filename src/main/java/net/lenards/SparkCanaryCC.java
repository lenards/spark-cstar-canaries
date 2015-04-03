package net.lenards;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SparkCanaryCC implements Serializable {

    private static void verifyArgs(String[] args) {
        if (args.length != 2) {
            System.err.println("Arguments required");
            System.exit(1);
        }
    }

    public static class SimplePair implements Serializable {
        private Integer key;
        private String value;

        public SimplePair() { /* Java Bean ctor */ }

        public SimplePair(Integer key, String value) {
            this.key = key;
            this.value = value;
        }

        public Integer getKey() { return key; }
        public void setKey(Integer key) { this.key = key; }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }

        @Override
        public String toString() {
            return MessageFormat.format("(key: {0}, val: {1})", key, value);
        }
    }

    public static void main(String[] args) {
        verifyArgs(args);

        SparkConf conf = new SparkConf()
            .setAppName("Spark Canary (CC) - Test")
            .setMaster(args[0])
            .set("spark.cassandra.connection.host", args[1]);
        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraConnector connector = CassandraConnector.apply(conf);

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS simple_canary_cc");
            session.execute("CREATE KEYSPACE simple_canary_cc WITH " +
                "REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE simple_canary_cc.simple_pair " +
                "(key int PRIMARY KEY, value text)");
        }

        List<SimplePair> pairs = Arrays.asList(
                new SimplePair(1, "One"),
                new SimplePair(5, "Five"),
                new SimplePair(6, "Six"),
                new SimplePair(7, "Seven"),
                new SimplePair(9, "Nine"),
                new SimplePair(10, "Ten"),
                new SimplePair(12, "Twelve"),
                new SimplePair(16, "Sixteen"),
                new SimplePair(19, "Nineteen")
        );


        JavaRDD<SimplePair> simplekvRDD = sc.parallelize(pairs);
        javaFunctions(simplekvRDD)
            .writerBuilder("simple_canary_cc", "simple_pair",
                           mapToRow(SimplePair.class))
            .saveToCassandra();

        CassandraJavaRDD<CassandraRow> rdd = javaFunctions(sc)
            .cassandraTable("simple_canary_cc", "simple_pair")
            .select("key", "value");


        long count = rdd.count();
        System.out.format("Count: %d %n", count);

        List somePairs = rdd.take(9);
        System.out.println(somePairs);

        sc.stop();
    }
}
