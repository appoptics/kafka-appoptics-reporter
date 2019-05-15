package com.appoptics.integrations.kafka.broker;

import java.util.EnumSet;
import java.util.Set;

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

    public final String displayName;

    public String buildMetricName(String metric) {
        return metric + "." + displayName;
    }

    ExpandedMetric(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Configures how to report "expanded" metrics derived from meters and histograms (e.g. percentiles,
     * rates, etc). Default is to report everything.
     */
    static class ExpandedMetricConfig {
        private final Set<ExpandedMetric> enabled;

        ExpandedMetricConfig(Set<ExpandedMetric> enabled) {
            this.enabled = EnumSet.copyOf(enabled);
        }

        boolean isSet(ExpandedMetric metric) {
            return enabled.contains(metric);
        }
    }
}
