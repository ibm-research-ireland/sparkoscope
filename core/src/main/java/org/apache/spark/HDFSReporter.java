package org.apache.spark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.spark.util.Utils;

import org.apache.hadoop.fs.FileSystem;

/**
 * Created by Yiannis Gkoufas on 17/09/15.
 */
public class HDFSReporter extends ScheduledReporter {

    private String executorId;
    private int rows = 0;

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link CsvReporter} with the given properties, writing {@code .csv} files to the
         * given directory.
         *
         * @param directory the directory in which the {@code .csv} files will be created
         * @return a {@link CsvReporter}
         */
        public HDFSReporter build(String directory) {
            return new HDFSReporter(registry,
                    directory,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSReporter.class);

    private final String directory;
    private String localhost = Utils.localHostName();
    private Configuration configuration;
    private FileSystem fileSystem;
    private FSDataOutputStream hadoopDataStream;
    private BufferedWriter writer;
    private final Clock clock;
    private long previousTimestamp = 0;
    private HashMap bufferEntries = new HashMap();

    private HDFSReporter(MetricRegistry registry,
                         String directory,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit,
                         Clock clock,
                         MetricFilter filter) {
        super(registry, "hdfs-reporter", filter, rateUnit, durationUnit);
        this.directory = directory;
        this.clock = clock;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp,
                name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
                "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s",
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        report(timestamp,
                name,
                "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit",
                "%d,%f,%f,%f,%f,events/%s",
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        report(timestamp,
                name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999",
                "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f",
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile());
    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        report(timestamp, name, "count", "%d", counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge) {
        report(timestamp, name, "value", "%s", gauge.getValue());
    }

    private void report(long timestamp, String name, String header, String line, Object... values) {
        try {
            String[] stringArr = name.split("\\.");
            String firstEntry = stringArr[0];
            String appId = "";
            if(!firstEntry.startsWith("app")) return;
            else appId = firstEntry;

            executorId = stringArr[1];

            try{
                Integer.parseInt(executorId);
            } catch (Exception e)
            {
                return;
            }

            if (previousTimestamp==0) {
                boolean directoryCreated = false;
                if(setHadoopConf())
                {
                    if(createWriter(appId,executorId))
                    {
                        directoryCreated = true;
                    }
                }
                if(!directoryCreated) return;
            }

            if (previousTimestamp == 0 || previousTimestamp == timestamp) {
                String[] keyArr = Arrays.copyOfRange(stringArr, 3, stringArr.length);
                List<String> entries = new ArrayList<String>();
                for (int i = 0; i < keyArr.length ; i++) {
                    entries.add(keyArr[i]);
                }
                putLeaf(entries,0,bufferEntries,values[0]);
            } else {

                rows++;

                HashMap finalMapToWrite = new HashMap();
                finalMapToWrite.put("timestamp",previousTimestamp);
                finalMapToWrite.put("values", bufferEntries);
                finalMapToWrite.put("host", localhost+"_"+executorId);
                String entryString = new JSONObject(finalMapToWrite).toString();
                writer.write(entryString);
                writer.newLine();

                if(rows%20==0)
                {
                    writer.flush();
                    hadoopDataStream.flush();
                    hadoopDataStream.hsync();
                }

                bufferEntries.clear();

                String[] keyArr = Arrays.copyOfRange(stringArr, 3, stringArr.length);
                List<String> entries = new ArrayList<String>();
                for (int i = 0; i < keyArr.length ; i++) {
                    entries.add(keyArr[i]);
                }
                putLeaf(entries, 0, bufferEntries, values[0]);
            }
            previousTimestamp = timestamp;
        } catch (Exception e) {
            LOGGER.warn("Error writing to {}", name, e);
        }
    }

    private boolean createWriter(String appId,String executorId) {
        Path appFolder = new Path(directory + File.separator + appId);
        try {
            if (!fileSystem.exists(appFolder)) {
                fileSystem.mkdirs(appFolder);
            }
            Path finalPath = new Path(directory + File.separator + appId + File.separator + localhost +"_"+executorId+".json");
            if (!fileSystem.exists(finalPath)) {
                fileSystem.createNewFile(finalPath);
            }
            hadoopDataStream = fileSystem.append(finalPath);
            writer = new BufferedWriter(new OutputStreamWriter(hadoopDataStream));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception when trying to create writer", e);
            return false;
        }
        return true;
    }

    private boolean setHadoopConf() {
        configuration = new Configuration();
        if(System.getenv().containsKey("HADOOP_CONF_DIR"))
        {
            String confDir = System.getenv().get("HADOOP_CONF_DIR");
            File file = new File(confDir);
            if(file.exists()&&file.isDirectory())
            {
                configuration.addResource(confDir);
            }
        }

        try {
            fileSystem = Utils.getHadoopFileSystem(new URI(directory), configuration);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception when trying set hadoop conf", e);
            return false;
        }
        return true;
    }

    protected String sanitize(String name) {
        return name;
    }

    public void putLeaf(List<String> entries, int index, HashMap originalMap, Object value)
    {
        String entry = entries.get(index);
        Object originalObj = originalMap.get(entry);
        if(originalObj instanceof HashMap || originalObj == null) {
            HashMap existing = (HashMap) originalObj;
            if (existing == null) existing = new HashMap();
            if (index == entries.size() - 1) {
                originalMap.put(entry, value);
            } else {
                originalMap.put(entry, existing);
                putLeaf(entries, index + 1, existing, value);
            }
        }
    }

    public void stop(){
        super.stop();
        try {
            if(writer!=null) {
                rows++;
                HashMap finalMapToWrite = new HashMap();
                finalMapToWrite.put("timestamp",previousTimestamp);
                finalMapToWrite.put("values", bufferEntries);
                finalMapToWrite.put("host", localhost+"_"+executorId);
                String entryString = new JSONObject(finalMapToWrite).toString();
                writer.write(entryString);
                writer.newLine();
                writer.flush();
                hadoopDataStream.flush();
                hadoopDataStream.hsync();
                writer.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception when flush writes to HDFS", e);
        }
    }

}
