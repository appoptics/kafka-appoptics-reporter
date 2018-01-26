package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A reporter for publishing metrics to <a href="http://metrics.librato.com/">Librato Metrics</a>
 */
public class Reporter extends AbstractPollingReporter implements MetricProcessor<KafkaMetricsBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(Reporter.class);
    private final DeltaTracker deltaTracker;
    private final String source;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final Sanitizer sanitizer;
    private final ScheduledExecutorService executor;
    private final String prefix;
    private final String prefixDelimiter;
    private final LibratoClient libratoClient;

    protected final MetricsRegistry registry;
    protected final MetricPredicate predicate;
    protected final Clock clock;
    protected final VirtualMachineMetrics vm;
    protected final boolean reportVmMetrics;
    protected final MetricExpansionConfig expansionConfig;

    private long interval = 30;

    /**
     * private to prevent someone from accidentally actually using this constructor. see .builder()
     */
    private Reporter(LibratoClient libratoClient,
                     String name,
                     final Sanitizer customSanitizer,
                     String source,
                     long timeout,
                     TimeUnit timeoutUnit,
                     final MetricsRegistry registry,
                     final MetricPredicate predicate,
                     Clock clock,
                     VirtualMachineMetrics vm,
                     boolean reportVmMetrics,
                     MetricExpansionConfig expansionConfig,
                     String prefix,
                     String prefixDelimiter) {
        super(registry, name);
        this.libratoClient = libratoClient;
        this.sanitizer = customSanitizer;
        this.source = source;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.registry = registry;
        this.predicate = predicate;
        this.clock = clock;
        this.vm = vm;
        this.reportVmMetrics = reportVmMetrics;
        this.expansionConfig = expansionConfig;
        this.executor = registry.newScheduledThreadPool(1, name);
        this.prefix = KafkaAppopticsUtil.checkPrefix(prefix);
        this.prefixDelimiter = prefixDelimiter;
        this.deltaTracker = new DeltaTracker(new Reporter.DeltaMetricSupplier(registry, predicate));
    }

    /**
     * Used to supply metrics to the delta tracker on initialization. Uses the metric name conversion
     * to ensure that the correct names are supplied for the metric.
     */
    class DeltaMetricSupplier implements DeltaTracker.MetricSupplier {
        final MetricsRegistry registry;
        final MetricPredicate predicate;

        DeltaMetricSupplier(MetricsRegistry registry, MetricPredicate predicate) {
            this.registry = registry;
            this.predicate = predicate;
        }

        public Map<String, Metric> getMetrics() {
            registry.groupedMetrics(predicate).entrySet();
            final Map<String, Metric> map = new HashMap<>();
            for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : registry.groupedMetrics(predicate).entrySet()) {
                for (Map.Entry<MetricName, Metric> metricEntry : entry.getValue().entrySet()) {
                    final String name = getStringName(metricEntry.getKey());
                    map.put(name, metricEntry.getValue());
                }
            }
            return map;
        }
    }

    @Override
    public void run() {
        // accumulate all the metrics in the batch, then post it allowing the AppopticsBatch class to break up the work
        KafkaMetricsBatch batch = new KafkaMetricsBatch(
                AppopticsBatch.DEFAULT_BATCH_SIZE,
                sanitizer,
                timeout,
                timeoutUnit,
                expansionConfig,
                prefix,
                prefixDelimiter,
                deltaTracker);
        if (reportVmMetrics) {
            reportVmMetrics(batch);
        }
        reportRegularMetrics(batch);
        try {
            Measures measures = new Measures(null, Collections.emptyList(), getEpoch(), (int) interval);
            for (Measurement m : batch.getMeasurements()) {
                String metricName = m.getName();
                Map<String, Number> values = m.toMap();
                if (values.size() == 1) {
                    Number value = values.get("value");
                    TaggedMeasure taggedMeasure = new TaggedMeasure(new GaugeMeasure(metricName, value.doubleValue()));
                    taggedMeasure.addTag(new Tag("source", source));
                    measures.add(taggedMeasure);
                } else if (values.size() == 4) {
                    Number count = values.get("count");
                    Number sum = values.get("sum");
                    Number min = values.get("min");
                    Number max = values.get("max");
                    TaggedMeasure taggedMeasure = new TaggedMeasure(new GaugeMeasure(metricName,
                            sum.doubleValue(),
                            count.longValue(),
                            min.doubleValue(),
                            max.doubleValue()));
                    taggedMeasure.addTag(new Tag("source", source));
                    measures.add(taggedMeasure);
                }
            }

            PostMeasuresResult result = libratoClient.postMeasures(measures);
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
        this.interval = period;
        executor.scheduleAtFixedRate(this, period, period, unit);
    }

    protected void reportVmMetrics(KafkaMetricsBatch batch) {
        addVmMetricsToBatch(vm, batch);
    }

    protected void reportRegularMetrics(KafkaMetricsBatch batch) {
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

    public void processGauge(MetricName name, Gauge<?> gauge, KafkaMetricsBatch batch) throws Exception {
        batch.addGauge(getStringName(name), gauge);
    }

    public void processCounter(MetricName name, Counter counter, KafkaMetricsBatch batch) throws Exception {
        batch.addCounter(getStringName(name), counter);
    }

    public void processHistogram(MetricName name, Histogram histogram, KafkaMetricsBatch batch) throws Exception {
        batch.addHistogram(getStringName(name), histogram);
    }

    public void processMeter(MetricName name, Metered meter, KafkaMetricsBatch batch) throws Exception {
        batch.addMetered(getStringName(name), meter);
    }

    public void processTimer(MetricName name, Timer timer, KafkaMetricsBatch batch) throws Exception {
        batch.addTimer(getStringName(name), timer);
    }

    private String getStringName(MetricName fullName) {
        return sanitizer.apply(KafkaAppopticsUtil.nameToString(fullName));
    }

    /**
     * a builder for the LibratoReporter class that requires things that cannot be inferred and uses
     * sane default values for everything else.
     */
    public static class Builder {
        private final String source;
        private final LibratoClient libratoClient;
        private Sanitizer sanitizer = Sanitizer.NO_OP;
        private long timeout = 5;
        private TimeUnit timeoutUnit = TimeUnit.SECONDS;
        private String name = "librato-reporter";
        private MetricsRegistry registry = Metrics.defaultRegistry();
        private MetricPredicate predicate = MetricPredicate.ALL;
        private Clock clock = Clock.defaultClock();
        private VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
        private boolean reportVmMetrics = true;
        private MetricExpansionConfig expansionConfig = MetricExpansionConfig.ALL;
        private String prefix;
        private String prefixDelimiter = ".";

        public Builder(String source, LibratoClient libratoClient) {
            this.source = source;
            this.libratoClient = libratoClient;
        }

        /**
         * Sets the character that will follow the prefix. Defaults to ".".
         *
         * @param delimiter the delimiter
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setPrefixDelimiter(String delimiter) {
            this.prefixDelimiter = delimiter;
            return this;
        }

        /**
         * Sets a prefix that will be prepended to all metric names
         *
         * @param prefix the prefix
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * set the HTTP timeout for a publishing attempt
         *
         * @param timeout     duration to expect a response
         * @param timeoutUnit unit for duration
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setTimeout(long timeout, TimeUnit timeoutUnit) {
            this.timeout = timeout;
            this.timeoutUnit = timeoutUnit;
            return this;
        }

        /**
         * Specify a custom name for this reporter
         *
         * @param name the name to be used
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Use a custom sanitizer. All metric names are run through a sanitizer to ensure validity before being sent
         * along. Librato places some restrictions on the characters allowed in keys, so all keys are ultimately run
         * through APIUtil.lastPassSanitizer. Specifying an additional sanitizer (that runs before lastPassSanitizer)
         * allows the user to customize what they want done about invalid characters and excessively long metric names.
         *
         * @param sanitizer the custom sanitizer to use  (defaults to a noop sanitizer).
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setSanitizer(Sanitizer sanitizer) {
            this.sanitizer = sanitizer;
            return this;
        }

        /**
         * override default MetricsRegistry
         *
         * @param registry registry to be used
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setRegistry(MetricsRegistry registry) {
            this.registry = registry;
            return this;
        }

        /**
         * Filter the metrics that this particular reporter publishes
         *
         * @param predicate the predicate by which the metrics are to be filtered
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setPredicate(MetricPredicate predicate) {
            this.predicate = predicate;
            return this;
        }

        /**
         * use a custom clock
         *
         * @param clock to be used
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * use a custom instance of VirtualMachineMetrics
         *
         * @param vm the instance to use
         * @return itself
         */
        @SuppressWarnings("unused")
        public Reporter.Builder setVm(VirtualMachineMetrics vm) {
            this.vm = vm;
            return this;
        }

        /**
         * turn on/off reporting of VM internal metrics (if, for example, you already get those elsewhere)
         *
         * @param reportVmMetrics true (report) or false (don't report)
         * @return itself
         */
        public Reporter.Builder setReportVmMetrics(boolean reportVmMetrics) {
            this.reportVmMetrics = reportVmMetrics;
            return this;
        }

        /**
         * Enables control over how the reporter generates 'expanded' metrics from meters and histograms,
         * such as percentiles and rates.
         *
         * @param expansionConfig the configuration
         * @return itself
         */
        public Reporter.Builder setExpansionConfig(MetricExpansionConfig expansionConfig) {
            this.expansionConfig = expansionConfig;
            return this;
        }

        /**
         * Build the LibratoReporter as configured by this Builder
         *
         * @return a fully configured LibratoReporter
         */
        public Reporter build() {
            return new Reporter(
                    libratoClient,
                    name,
                    sanitizer,
                    source,
                    timeout,
                    timeoutUnit,
                    registry,
                    predicate,
                    clock,
                    vm,
                    reportVmMetrics,
                    expansionConfig,
                    prefix,
                    prefixDelimiter);
        }
    }

    public static Builder builder(String source, LibratoClient libratoClient) {
        return new Builder(source, libratoClient);
    }

    public enum ExpandedMetric {
        // sampling
        MEDIAN("median"),
        PCT_75("75th"),
        PCT_95("95th"),
        PCT_98("98th"),
        PCT_99("99th"),
        PCT_999("999th"),
        // metered
        COUNT("count"),
        RATE_MEAN("meanRate"),
        RATE_1_MINUTE("1MinuteRate"),
        RATE_5_MINUTE("5MinuteRate"),
        RATE_15_MINUTE("15MinuteRate");

        private final String displayName;

        public String buildMetricName(String metric) {
            return metric + "." + displayName;
        }

        ExpandedMetric(String displayName) {
            this.displayName = displayName;
        }
    }

    /**
     * Configures how to report "expanded" metrics derived from meters and histograms (e.g. percentiles,
     * rates, etc). Default is to report everything.
     */
    public static class MetricExpansionConfig {
        public static MetricExpansionConfig ALL = new MetricExpansionConfig(EnumSet.allOf(ExpandedMetric.class));
        private final Set<ExpandedMetric> enabled;

        public MetricExpansionConfig(Set<ExpandedMetric> enabled) {
            this.enabled = EnumSet.copyOf(enabled);
        }

        public boolean isSet(ExpandedMetric metric) {
            return enabled.contains(metric);
        }
    }

    /**
     * helper method for adding VM metrics to a batch
     */
    public static void addVmMetricsToBatch(VirtualMachineMetrics vm, KafkaMetricsBatch batch) {
        // memory
        batch.addGaugeMeasurement("kafka.server.jvm.memory.heap_usage", vm.heapUsage());
        batch.addGaugeMeasurement("kafka.server.jvm.memory.non_heap_usage", vm.nonHeapUsage());
        for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            batch.addGaugeMeasurement("kafka.server.jvm.memory.memory_pool_usages." + pool.getKey(), pool.getValue());
        }

        // threads
        batch.addGaugeMeasurement("kafka.server.jvm.daemon_thread_count", vm.daemonThreadCount());
        batch.addGaugeMeasurement("kafka.server.jvm.thread_count", vm.threadCount());
        batch.addGaugeMeasurement("kafka.server.jvm.uptime", vm.uptime());
        batch.addGaugeMeasurement("kafka.server.jvm.fd_usage", vm.fileDescriptorUsage());

        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            batch.addGaugeMeasurement("kafka.server.jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue());
        }

        // garbage collection
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "kafka.server.jvm.gc." + entry.getKey();
            batch.addCounterMeasurement(name + ".time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            batch.addCounterMeasurement(name + ".runs", entry.getValue().getRuns());
        }
    }
}

