package com.appoptics.integrations.kafka.broker;

import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

/**
 * a AppopticsBatch that understands Metrics-specific types
 */
class KafkaMetricsBatch {
    final List<Measurement> measurements = new ArrayList<>();

    private final ExpandedMetric.ExpandedMetricConfig expansionConfig;
    private final DeltaTracker deltaTracker;

    /**
     * Public constructor.
     */
    KafkaMetricsBatch(ExpandedMetric.ExpandedMetricConfig expansionConfig,
                      DeltaTracker deltaTracker) {
        this.expansionConfig = Preconditions.checkNotNull(expansionConfig);
        this.deltaTracker = deltaTracker;
    }

    private void addMeasurement(Measurement measurement) {
        measurements.add(measurement);
    }

    void addGaugeMeasurement(NameAndTags nameAndTags, Number value) {
        addMeasurement(new SimpleMeasurement(nameAndTags, value));
    }

    void addGaugeMeasurement(String name, Number value) {
        addMeasurement(new SimpleMeasurement(name, value));
    }

    // begin direct support for Coda Metrics

    void addGauge(NameAndTags nameAndTags, Gauge gauge) {
        final Object value = gauge.value();
        if (value instanceof Number) {
            final Number number = (Number)value;
            if (isANumber(number)) {
                addGaugeMeasurement(nameAndTags, number);
            }
        }
    }

    void addCounter(NameAndTags nameAndTags, Counter counter) {
        final Long countDelta = deltaTracker.getDelta(nameAndTags, counter.count());
        addGaugeMeasurement(nameAndTags, countDelta);
    }

    void addHistogram(NameAndTags nameAndTags, Histogram histogram) {
        final Long countDelta = deltaTracker.getDelta(nameAndTags, histogram.count());
        maybeAdd(ExpandedMetric.COUNT, nameAndTags, countDelta);
        addSummarizable(nameAndTags, histogram);
        addSampling(nameAndTags, histogram);
    }

    void addMetered(NameAndTags nameAndTags, Metered meter) {
        final Long deltaCount = deltaTracker.getDelta(nameAndTags, meter.count());
        maybeAdd(ExpandedMetric.COUNT, nameAndTags, deltaCount);
        maybeAdd(ExpandedMetric.RATE_MEAN, nameAndTags, meter.meanRate());
        maybeAdd(ExpandedMetric.RATE_1_MINUTE, nameAndTags, meter.oneMinuteRate());
        maybeAdd(ExpandedMetric.RATE_5_MINUTE, nameAndTags, meter.fiveMinuteRate());
        maybeAdd(ExpandedMetric.RATE_15_MINUTE, nameAndTags, meter.fifteenMinuteRate());
    }

    void addTimer(NameAndTags nameAndTags, Timer timer) {
        addMetered(nameAndTags, timer);
        addSummarizable(nameAndTags, timer);
        addSampling(nameAndTags, timer);
    }

    private void addSummarizable(NameAndTags nameAndTags, Summarizable summarizable) {
        // TODO: add sum_squares if/when Summarizable exposes it
        final double countCalculation = summarizable.sum() / summarizable.mean();
        Long countValue = null;
        if (isANumber(countCalculation)) {
            countValue = Math.round(countCalculation);
        }
        // no need to publish these additional values if they are zero, plus the API will puke
        if (countValue != null && countValue > 0) {
            addMeasurement(new MultiSampleMeasurement(
                    nameAndTags,
                    countValue,
                    summarizable.sum(),
                    summarizable.max(),
                    summarizable.min()
            ));
        }
    }

    private void addSampling(NameAndTags nameAndTags, Sampling sampling) {
        final Snapshot snapshot = sampling.getSnapshot();
        maybeAdd(ExpandedMetric.MEDIAN, nameAndTags, snapshot.getMedian());
        maybeAdd(ExpandedMetric.PCT_75, nameAndTags, snapshot.get75thPercentile());
        maybeAdd(ExpandedMetric.PCT_95, nameAndTags, snapshot.get95thPercentile());
        maybeAdd(ExpandedMetric.PCT_98, nameAndTags, snapshot.get98thPercentile());
        maybeAdd(ExpandedMetric.PCT_99, nameAndTags, snapshot.get99thPercentile());
        maybeAdd(ExpandedMetric.PCT_999, nameAndTags, snapshot.get999thPercentile());
    }

    private void maybeAdd(ExpandedMetric metric, NameAndTags nameAndTags, Number reading) {
        if (expansionConfig.isSet(metric)) {
            addGaugeMeasurement(nameAndTags.withSuffix(metric.displayName), reading);
        }
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

