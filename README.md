# Spark + C* Canaries

Simple Java applications that use the [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) to verify deployment configuration.

*Note: **Available as is.** Only for demonstration use for development configuration verification.*

## Build

Project depends on [Maven](https://maven.apache.org/) 3.* for packaging the jar.

```
$ mvn clean package
```

## Canary: Simple Use Case

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

## Canary: Amazon Kinesis + DSE

With a [DataStax Enterprise 4.6.*](http://datastax.com/downloads/), there is *currently* (as of May 11, 2015) a conflict in jar dependencies with the Amazon Kinesis Client and the resources integrated for Spark with DataStax Enterprise. A step-by-step guide for working around this is provided in the [Wiki](https://github.com/lenards/spark-cstar-canaries/wiki) fort his repository.
