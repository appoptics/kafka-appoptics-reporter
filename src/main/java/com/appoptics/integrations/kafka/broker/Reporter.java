package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter for publishing metrics to <a href="https://appoptics.com/">Appoptics Metrics</a>
 */
public class Reporter extends AbstractPollingReporter implements MetricProcessor<KafkaMetricsBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(Reporter.class);
    private static final String NAME = "kafkaappoptics-reporter";

    private static final NameAndTags HEAP_USAGE = new NameAndTags("kafka.server.jvm.memory.heap_usage");
    private static final NameAndTags NON_HEAP_USAGE = new NameAndTags("kafka.server.jvm.memory.non_heap_usage");
    private static final NameAndTags DAEMON_THREAD_COUNT = new NameAndTags("kafka.server.jvm.daemon_thread_count");
    private static final NameAndTags THREAD_COUNT = new NameAndTags("kafka.server.jvm.thread_count");
    private static final NameAndTags UPTIME = new NameAndTags("kafka.server.jvm.uptime");
    private static final NameAndTags FD_USAGE = new NameAndTags("kafka.server.jvm.fd_usage");

    private final DeltaTracker deltaTracker;
    private final AppopticsClient appopticsClient;
    private final List<Tag> tags;

    private final MetricPredicate predicate = MetricPredicate.ALL;
    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
    private final ExpandedMetric.ExpandedMetricConfig expansionConfig;

    private long interval = 30;

    public Reporter(AppopticsClient appopticsClient,
                    ExpandedMetric.ExpandedMetricConfig expansionConfig,
                    List<Tag> tags) {
        super(Metrics.defaultRegistry(), NAME);
        this.appopticsClient = appopticsClient;
        this.expansionConfig = expansionConfig;
        this.deltaTracker = new DeltaTracker(new DeltaMetricSupplier(getMetricsRegistry(), predicate));
        this.tags = tags;
    }

    @Override
    public void run() {
        // accumulate all the metrics in the batch, then post it allowing the AppopticsBatch class to break up the work
        KafkaMetricsBatch batch = new KafkaMetricsBatch(expansionConfig, deltaTracker);
        reportVmMetrics(batch);
        reportRegularMetrics(batch);

        try {
            Measures measures = new Measures(Collections.emptyList(), getEpoch(), (int) interval);
            batch.measurements.forEach(m -> measures.add(m.asMeasure(tags)));

            PostMeasuresResult result = appopticsClient.postMeasures(measures);
            for (PostResult r : result.results) {
                if (r.isError()) {
                    if (r.response != null) {
                        LOG.error(String.format("error attempting to post measurements to librato, response code %s",
                                r.response.getResponseCode()), r.exception);
                    } else {
                        LOG.error("error attempting to post measurements to librato", r.exception);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Librato post failed: ", e);
        }
    }

    private long getEpoch() {
        long epochSecond = Instant.now().getEpochSecond();
        return (epochSecond / interval) * interval;
    }

    /**
     * Starts the reporter polling at the given period.
     *
     * @param period the amount of time between polls
     * @param unit   the unit for {@code period}
     */
    @Override
    public void start(long period, TimeUnit unit) {
        LOG.debug("Reporter starting at fixed rate of every {} {}", period, unit);
        this.interval = unit.toSeconds(period);
        super.start(period, unit);
    }

    private void reportVmMetrics(KafkaMetricsBatch batch) {
        addVmMetricsToBatch(vm, batch);
    }

    private void reportRegularMetrics(KafkaMetricsBatch batch) {
        final SortedMap<String, SortedMap<MetricName, Metric>> metrics = getMetricsRegistry().groupedMetrics(predicate);
        LOG.debug("Preparing batch of {} top level metrics", metrics.size());
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : metrics.entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), batch);
                    } catch (Exception e) {
                        LOG.error("Error processing regular metrics:", e);
                    }
                }
            }
        }
    }

    public void processGauge(MetricName name, Gauge<?> gauge, KafkaMetricsBatch batch) {
        NameAndTags nameAndTags = new NameAndTags(name);
        batch.addGauge(nameAndTags, gauge);
    }

    public void processCounter(MetricName name, Counter counter, KafkaMetricsBatch batch) {
        NameAndTags nameAndTags = new NameAndTags(name);
        batch.addCounter(nameAndTags, counter);
    }

    public void processHistogram(MetricName name, Histogram histogram, KafkaMetricsBatch batch) {
        NameAndTags nameAndTags = new NameAndTags(name);
        batch.addHistogram(nameAndTags, histogram);
    }

    public void processMeter(MetricName name, Metered meter, KafkaMetricsBatch batch) {
        NameAndTags nameAndTags = new NameAndTags(name);
        batch.addMetered(nameAndTags, meter);
    }

    public void processTimer(MetricName name, Timer timer, KafkaMetricsBatch batch) {
        NameAndTags nameAndTags = new NameAndTags(name);
        batch.addTimer(nameAndTags, timer);
    }

    /**
     * helper method for adding VM metrics to a batch
     */
    private void addVmMetricsToBatch(VirtualMachineMetrics vm, KafkaMetricsBatch batch) {

        // memory
        batch.addGaugeMeasurement(HEAP_USAGE, vm.heapUsage());
        batch.addGaugeMeasurement(NON_HEAP_USAGE, vm.nonHeapUsage());
        for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            batch.addGaugeMeasurement("kafka.server.jvm.memory.memory_pool_usages." + pool.getKey(), pool.getValue());
        }

        // threads
        batch.addGaugeMeasurement(DAEMON_THREAD_COUNT, vm.daemonThreadCount());
        batch.addGaugeMeasurement(THREAD_COUNT, vm.threadCount());
        batch.addGaugeMeasurement(UPTIME, vm.uptime());
        batch.addGaugeMeasurement(FD_USAGE, vm.fileDescriptorUsage());

        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            batch.addGaugeMeasurement("kafka.server.jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue());
        }

        // garbage collection
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "kafka.server.jvm.gc." + entry.getKey();
            batch.addGaugeMeasurement(name + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            batch.addGaugeMeasurement(name + ".runs", entry.getValue().getRuns());
        }
    }
}

