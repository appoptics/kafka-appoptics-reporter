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
        registry.groupedMetrics(predicate).values().forEach(smap -> smap.forEach((name, metric) -> {
            NameAndTags nameAndTags = new NameAndTags(name);
            map.put(nameAndTags, metric);
        }));
        return map;
    }
}
