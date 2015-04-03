# Spark + C* Canaries

Simple Java applications that use the [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) to verify deployment configuration.

*Note: **Available as is.** Only for demonstration use for development configuration verification.*

## Build

Project depends on [Maven](https://maven.apache.org/) 3.* for packaging the jar.

```
$ mvn clean package
```

## Use

With a [DataStax Enterprise](http://datastax.com/downloads/) cluster, you can submit the jar using [``dse spark-submit``](http://docs.datastax.com/en/datastax_enterprise/4.6/datastax_enterprise/spark/sparkStart.html?scroll=sparkStart__dseSparkSubmit):
```
$ cd $REPO
$ dse spark-submit --class net.lenards.SparkCanaryCC \
  target/spark-cstar-canaries-0.0.1-SNAPSHOT.jar $(dsetool sparkmaster) 127.0.0.1
```

The expected output would be:
```
Count: 9
[CassandraRow{key: 7, value: Seven}, CassandraRow{key: 6, value: Six}, CassandraRow{key: 9, value: Nine}, CassandraRow{key: 12, value: Twelve}, CassandraRow{key: 5, value: Five}, CassandraRow{key: 10, value: Ten}, CassandraRow{key: 16, value: Sixteen}, CassandraRow{key: 1, value: One}, CassandraRow{key: 19, value: Nineteen}]
```
