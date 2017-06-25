package org.apache.spark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.apache.spark.util.Utils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by johngouf on 5/24/17.
 */
public class MQTTReporter extends ScheduledReporter {

    private String executorId;
    private String appId;

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
         * @param masterHost
         * @param masterMqttPort
         * @return a {@link CsvReporter}
         */
        public MQTTReporter build(String masterHost, int masterMqttPort) {
            return new MQTTReporter(registry,
                    masterHost,
                    masterMqttPort,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MQTTReporter.class);

    private final String masterHost;
    private final int masterMqttPort;
    private String localhost = Utils.localHostName();
    private MqttClient mqttClient;

    private final Clock clock;
    private long previousTimestamp = 0;
    private HashMap bufferEntries = new HashMap();

    private MQTTReporter(MetricRegistry registry,
                         String masterHost,
                         int masterMqttPort,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit,
                         Clock clock,
                         MetricFilter filter) {
        super(registry, "mqtt-reporter", filter, rateUnit, durationUnit);
        this.masterHost = masterHost;
        this.masterMqttPort = masterMqttPort;
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
            appId = "";
            if (!firstEntry.startsWith("app")) return;
            else appId = firstEntry;

            executorId = stringArr[1];

            try {
                Integer.parseInt(executorId);
            } catch (Exception e) {
                return;
            }
            if (previousTimestamp==0) {
                createMqttClient(appId,executorId);
            }

            if (previousTimestamp == 0 || previousTimestamp == timestamp) {
                String[] keyArr = Arrays.copyOfRange(stringArr, 3, stringArr.length);
                List<String> entries = new ArrayList<String>();
                for (int i = 0; i < keyArr.length; i++) {
                    entries.add(keyArr[i]);
                }
                putLeaf(entries, 0, bufferEntries, values[0]);
            } else {

                HashMap finalMapToWrite = new HashMap();
                finalMapToWrite.put("timestamp", previousTimestamp);
                finalMapToWrite.put("values", bufferEntries);
                finalMapToWrite.put("host", localhost + "_" + executorId);
                String entryString = new JSONObject(finalMapToWrite).toString();
                MqttMessage message = new MqttMessage(entryString.getBytes());
                message.setQos(2);
                mqttClient.publish("metrics-"+appId, message);
                bufferEntries.clear();

                String[] keyArr = Arrays.copyOfRange(stringArr, 3, stringArr.length);
                List<String> entries = new ArrayList<String>();
                for (int i = 0; i < keyArr.length; i++) {
                    entries.add(keyArr[i]);
                }
                putLeaf(entries, 0, bufferEntries, values[0]);
            }
            previousTimestamp = timestamp;
        } catch (Exception e) {
            LOGGER.warn("Error writing to {}", name, e);
        }
    }

    private boolean createMqttClient(String appId, String executorId) {
        System.out.println("Creating client for " + appId + " and " + executorId);
        try {
            String broker = "tcp://"+masterHost+":"+masterMqttPort;
            MemoryPersistence persistence = new MemoryPersistence();
            mqttClient = new MqttClient(
                    "tcp://"+masterHost+":"+masterMqttPort,
                    appId+"-"+localhost+"-"+executorId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            mqttClient.connect(connOpts);
            System.out.println("Connecting to broker: "+broker);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception when trying to create the client", e);
            return false;
        }
        return true;
    }

    protected String sanitize(String name) {
        return name;
    }

    public void putLeaf(List<String> entries, int index, HashMap originalMap, Object value) {
        String entry = entries.get(index);
        Object originalObj = originalMap.get(entry);
        if (originalObj instanceof HashMap || originalObj == null) {
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

    public void stop() {
        super.stop();
        try {
                HashMap finalMapToWrite = new HashMap();
                finalMapToWrite.put("timestamp", previousTimestamp);
                finalMapToWrite.put("values", bufferEntries);
                finalMapToWrite.put("host", localhost + "_" + executorId);
                String entryString = new JSONObject(finalMapToWrite).toString();
                MqttMessage message = new MqttMessage(entryString.getBytes());
                message.setQos(2);
                mqttClient.publish("metrics-"+appId, message);
                mqttClient.disconnect();
            }
            catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception when flushing", e);
        }
    }

}
