# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and structured
data processing, MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html)
and [project wiki](https://cwiki.apache.org/confluence/display/SPARK).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark
    
And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL, 
"yarn-cluster" or "yarn-client" to run on YARN, and "local" to run 
locally with one thread, or "local[N]" to run locally with N threads. You 
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to 
[run all automated tests](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-AutomatedTesting).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions. See also
["Third Party Hadoop Distributions"](http://spark.apache.org/docs/latest/hadoop-third-party-distributions.html)
for guidance on building a Spark application that works with a particular
distribution.

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

# Spark Viz

## Installation/Configuration

In all the nodes of the cluster Hyperic Sigar library must be installed. 
Download from [http://sourceforge.net/projects/sigar/files/sigar/1.6/hyperic-sigar-1.6.4.zip/download](http://sourceforge.net/projects/sigar/files/sigar/1.6/hyperic-sigar-1.6.4.zip/download). 
Extract the zip in any location.

In spark-env.sh you need to set the HADOOP_CONF_DIR variable to the configuration directory of your hadoop installation. For instance:

```
HADOOP_CONF_DIR=/disk/hadoop-2.7.0/etc/hadoop
```

Sigar metrics can be used as a source in the metrics.properties file:

```
sigar.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
```

```
sigar.sink.csv.period=20
```

The above configuration will output the Sigar metrics in csv files. Check the metrics.properties documentation for additional parameters of CsvSink.

In order for the metrics to be stored in HDFS and therefore by retrieved by the UI, you need to have the following in the metrics.properties file:

```
sigar.sink.hdfs.class=org.apache.spark.metrics.sink.SigarSink
```

```
sigar.sink.hdfs.pollPeriod = 20
```

```
sigar.sink.hdfs.dir = hdfs://localhost:9000/custom-metrics
```

```
sigar.sink.hdfs.unit = seconds
```

CsvSink is not required for the UI to display the metrics.

In spark-defaults.conf you need to add to java.library.path parameter of the executor the directory of the native libraries of sigar. For instance:

```
spark.executor.extraJavaOptions -Djava.library.path=/disk/hyperic-sigar-1.6.4/sigar-bin/lib/
```

Event logging must be enabled in spark-defaults.conf:

```
spark.eventLog.enabled           true
```
```
spark.eventLog.dir               hdfs://127.0.0.1:9000/spark-logs
```

Also in spark-defaults.conf you should specify the folder from which the UI will read the metrics:

```
spark.sigar.dir                  hdfs://127.0.0.1:9000/custom-metrics
```

# **Important**: 

The folders spark.eventLog.dir, sigar.sink.hdfs.dir and spark.sigar.dir must already exist in the HDFS.

You should increase the limit for open files on the operating systems of the Master and the Workers

## Tested Environments

1. ~~Standalone Cluster Mode~~
2. ~~Hadoop 2.7.0~~
3. YARN
4. Mesos

