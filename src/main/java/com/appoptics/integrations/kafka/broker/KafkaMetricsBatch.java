package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.Sanitizer;
import com.librato.metrics.client.Versions;
import com.librato.metrics.reporter.LibratoReporter;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

/**
 * a AppopticsBatch that understands Metrics-specific types
 */
public class KafkaMetricsBatch extends AppopticsBatch {
    private final Reporter.MetricExpansionConfig expansionConfig;
    private final String prefix;
    private final String prefixDelimiter;
    private final DeltaTracker deltaTracker;

    /**
     * a string used to identify the library
     */
    private static final String AGENT_IDENTIFIER;

    static {
        final String version = Versions.getVersion("META-INF/maven/com.librato.metrics/metrics-librato/pom.properties", LibratoReporter.class);
        final String codaVersion = Versions.getVersion("META-INF/maven/com.yammer.metrics/metrics-core/pom.properties", MetricsRegistry.class);
        AGENT_IDENTIFIER = String.format("metrics-librato/%s metrics/%s", version, codaVersion);
    }

    /**
     * Public constructor.
     */
    public KafkaMetricsBatch(int postBatchSize,
                               Sanitizer sanitizer,
                               long timeout,
                               TimeUnit timeoutUnit,
                               Reporter.MetricExpansionConfig expansionConfig,
                               String prefix,
                               String delimiter,
                               DeltaTracker deltaTracker) {
        super(postBatchSize, sanitizer, timeout, timeoutUnit, AGENT_IDENTIFIER);
        this.expansionConfig = Preconditions.checkNotNull(expansionConfig);
        this.prefix = KafkaAppopticsUtil.checkPrefix(prefix);
        this.prefixDelimiter = delimiter;
        this.deltaTracker = deltaTracker;
    }

    public List<Measurement> getMeasurements() {
        return measurements;
    }

    @Override
    public void addCounterMeasurement(String name, Long value) {
        super.addCounterMeasurement(addPrefix(name), value);
    }

    @Override
    public void addGaugeMeasurement(String name, Number value) {
        super.addGaugeMeasurement(addPrefix(name), value);
    }

    // begin direct support for Coda Metrics

    public void addGauge(String name, Gauge gauge) {
        final Object value = gauge.value();
        if (value instanceof Number) {
            final Number number = (Number)value;
            if (isANumber(number)) {
                addGaugeMeasurement(name, number);
            }
        }
    }

    public void addCounter(String name, Counter counter) {
        addGaugeMeasurement(name, counter.count());
    }

    public void addHistogram(String name, Histogram histogram) {
        final Long countDelta = deltaTracker.getDelta(name, histogram.count());
        maybeAdd(Reporter.ExpandedMetric.COUNT, name, countDelta);
        addSummarizable(name, histogram);
        addSampling(name, histogram);
    }

    public void addMetered(String name, Metered meter) {
        final Long deltaCount = deltaTracker.getDelta(name, meter.count());
        maybeAdd(Reporter.ExpandedMetric.COUNT, name, deltaCount);
        maybeAdd(Reporter.ExpandedMetric.RATE_MEAN, name, meter.meanRate());
        maybeAdd(Reporter.ExpandedMetric.RATE_1_MINUTE, name, meter.oneMinuteRate());
        maybeAdd(Reporter.ExpandedMetric.RATE_5_MINUTE, name, meter.fiveMinuteRate());
        maybeAdd(Reporter.ExpandedMetric.RATE_15_MINUTE, name, meter.fifteenMinuteRate());
    }

    public void addTimer(String name, Timer timer) {
        addMetered(name, timer);
        addSummarizable(name, timer);
        addSampling(name, timer);
    }

    private void addSummarizable(String name, Summarizable summarizable) {
        // TODO: add sum_squares if/when Summarizable exposes it
        final double countCalculation = summarizable.sum() / summarizable.mean();
        Long countValue = null;
        if (isANumber(countCalculation)) {
            countValue = Math.round(countCalculation);
        }
        // no need to publish these additional values if they are zero, plus the API will puke
        if (countValue != null && countValue > 0) {
            addMeasurement(new MultiSampleGaugeMeasurement(
                    name,
                    countValue,
                    summarizable.sum(),
                    summarizable.max(),
                    summarizable.min(),
                    null
            ));
        }
    }

    public void addSampling(String name, Sampling sampling) {
        final Snapshot snapshot = sampling.getSnapshot();
        maybeAdd(Reporter.ExpandedMetric.MEDIAN, name, snapshot.getMedian());
        maybeAdd(Reporter.ExpandedMetric.PCT_75, name, snapshot.get75thPercentile());
        maybeAdd(Reporter.ExpandedMetric.PCT_95, name, snapshot.get95thPercentile());
        maybeAdd(Reporter.ExpandedMetric.PCT_98, name, snapshot.get98thPercentile());
        maybeAdd(Reporter.ExpandedMetric.PCT_99, name, snapshot.get99thPercentile());
        maybeAdd(Reporter.ExpandedMetric.PCT_999, name, snapshot.get999thPercentile());
    }

    private void maybeAdd(Reporter.ExpandedMetric metric, String name, Number reading) {
        if (expansionConfig.isSet(metric)) {
            addGaugeMeasurement(metric.buildMetricName(name), reading);
        }
    }

    private String addPrefix(String metricName) {
        if (prefix == null || prefix.length() == 0) {
            return metricName;
        }
        return prefix + prefixDelimiter + metricName;
    }

    /**
     * Ensures that a number's value is an actual number
     *
     * @param number the number
     * @return true if the number is not NaN or infinite, false otherwise
     */
    private boolean isANumber(Number number) {
        final double doubleValue = number.doubleValue();
        return !(Double.isNaN(doubleValue) || Double.isInfinite(doubleValue));
    }


    static final class Preconditions {
        private Preconditions() {
            // helper class, do not instantiate
        }

        static Number checkNumeric(Number number) {
            if (number == null) {
                return null;
            }
            final double doubleValue = number.doubleValue();
            if (isNaN(doubleValue) || isInfinite(doubleValue)) {
                throw new IllegalArgumentException(number + " is not a numeric value");
            }
            return number;
        }

        static <T> T checkNotNull(T object) {
            if (object == null) {
                throw new IllegalArgumentException("Parameter may not be null");
            }
            return object;
        }
    }
}

