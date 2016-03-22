# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
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

    build/mvn -DskipTests clean package

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
"yarn" to run on YARN, and "local" to run
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
[run tests for a module, or individual tests](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

# SparkOscope

## Installation/Configuration

**Configure Sigar metrics source**

In all the nodes of the cluster Hyperic Sigar library must be installed. 
Download from [http://sourceforge.net/projects/sigar/files/sigar/1.6/hyperic-sigar-1.6.4.zip/download](http://sourceforge.net/projects/sigar/files/sigar/1.6/hyperic-sigar-1.6.4.zip/download). 
Extract the zip in any location.

In spark-envh.sh you need to add to LD_LIBRARY_PATH variable the directory of the native libraries of Sigar. For instance:

```
LD_LIBRARY_PATH=/path/to/hyperic-sigar-1.6.4/sigar-bin/lib/:$LD_LIBRARY_PATH
```

Add the source definition to *metrics.properties*

```
executor.source.jvm.class=org.apache.spark.metrics.source.SigarSource
```

**Configure hadoop**

In spark-env.sh you need to set the HADOOP_CONF_DIR variable to the configuration directory of your hadoop installation. For instance:

```
HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop
```

**Configure hdfs metrics sink**

In order for the executor metrics to be stored in HDFS and therefore be retrieved by the UI, you need to have the following in the *metrics.properties* file:

```
executor.sink.hdfs.class=org.apache.spark.metrics.sink.HDFSSink
```

```
executor.sink.hdfs.pollPeriod = 20
```

```
executor.sink.hdfs.dir = hdfs://localhost:9000/custom-metrics
```

```
executor.sink.hdfs.unit = seconds
```

**Event and UI configuration**

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

## Notes about the metrics

The metrics are grouped per application and the user can access the plots by selecting the **Name** entry under the **Completed Applications** table.
The URL on the browser should look similar to http://ip-of-spark-master:port/history/app-201511XXXXXX-XXX
Under the dropdown menu **Executor Metrics** the user can plot any of the metrics provided per executor but also metrics of the operating system of the host (physical or virtual):

### sigar.ram

Percentage of RAM utilization

### sigar.cpu

Percentage of CPU utilization

### sigar.kBytesRxPerSecond / sigar.kBytesTxPerSecond

Number of Kilobytes received/transmitted from/to the network per second

### sigar.kBytesReadPerSecond / sigar.kBytesWrittenPerSecond

Number of Kilobytes read/written from/to the disk per second


# **Important**: 

The folders spark.eventLog.dir, sigar.sink.hdfs.dir and spark.sigar.dir must already exist in the HDFS.

You should increase the limit for open files on the operating systems of the Master and the Workers.

Be sure to build spark according to the version of hadoop you are using.
