package com.appoptics.integrations.kafka.broker;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Used to supply metrics to the delta tracker on initialization. Uses the metric NAME conversion
 * to ensure that the correct names are supplied for the metric.
 */
class DeltaMetricSupplier implements DeltaTracker.MetricSupplier {
    private final MetricsRegistry registry;
    private final MetricPredicate predicate;

    DeltaMetricSupplier(MetricsRegistry registry, MetricPredicate predicate) {
        this.registry = registry;
        this.predicate = predicate;
    }

    public Map<NameAndTags, Metric> getMetrics() {
        final Map<NameAndTags, Metric> map = new HashMap<>();
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : registry.groupedMetrics(predicate).entrySet()) {
            for (Map.Entry<MetricName, Metric> metricEntry : entry.getValue().entrySet()) {
                NameAndTags nameAndTags = new NameAndTags(metricEntry.getKey());
                map.put(nameAndTags, metricEntry.getValue());
            }
        }
        return map;
    }
}
