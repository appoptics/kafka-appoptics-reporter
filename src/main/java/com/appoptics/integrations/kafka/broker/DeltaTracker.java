package com.appoptics.integrations.kafka.broker;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks the last named value.
 */
class DeltaTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaTracker.class);
    private final Map<NameAndTags, Long> lookup = new HashMap<>();

    public interface MetricSupplier {
        Map<NameAndTags, Metric> getMetrics();
    }

    DeltaTracker(MetricSupplier supplier) {
        for (Map.Entry<NameAndTags, Metric> entry : supplier.getMetrics().entrySet()) {
            final NameAndTags nameAndTags = entry.getKey();
            final Metric metric = entry.getValue();
            if (metric instanceof Metered) {
                lookup.put(nameAndTags, ((Metered) metric).count());
            }
            if (metric instanceof Histogram) {
                lookup.put(nameAndTags, ((Histogram) metric).count());
            }
        }
    }

    /**
     * Calculates the delta.  If this is a new value that has not been seen before, zero will be assumed to be the
     * initial value.
     *
     * @param nameAndTags  the nameAndTags of the counter
     * @param count the counter value
     * @return the delta
     */
    Long getDelta(NameAndTags nameAndTags, long count) {
        Long previous = lookup.put(nameAndTags, count);
        if (previous == null) {
            // this is the first time we have seen this count
            previous = 0L;
        }
        if (count < previous) {
            LOG.error("Saw a non-monotonically increasing value for metric {}", nameAndTags);
            return 0L;
        }
        return count - previous;
    }
}

