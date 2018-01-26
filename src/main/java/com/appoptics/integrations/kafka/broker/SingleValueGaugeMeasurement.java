package com.appoptics.integrations.kafka.broker;

import java.util.HashMap;
import java.util.Map;

/**
 * A class representing a single gauge reading
 * <p/>
 * See http://dev.librato.com/v1/post/metrics for an explanation of basic vs multi-sample gauge
 */
public class SingleValueGaugeMeasurement implements Measurement {
    private final String name;
    private final Number reading;

    public SingleValueGaugeMeasurement(String name, Number reading) {
        try {
            this.name = KafkaMetricsBatch.Preconditions.checkNotNull(name);
            this.reading = KafkaMetricsBatch.Preconditions.checkNumeric(KafkaMetricsBatch.Preconditions.checkNumeric(reading));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid single-gauge measurement name=" + name, e);
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, Number> toMap() {
        final Map<String, Number> value = new HashMap<String, Number>();
        value.put("value", reading);
        return value;
    }
}

