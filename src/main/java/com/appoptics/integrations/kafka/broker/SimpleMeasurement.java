package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.Measure;
import com.appoptics.metrics.client.Tag;

import java.util.List;

/**
 * A class representing a single gauge reading
 * <p/>
 * See http://dev.librato.com/v1/post/metrics for an explanation of basic vs multi-sample gauge
 */
public class SimpleMeasurement extends Measurement {
    private final Number reading;

    SimpleMeasurement(NameAndTags nameAndTags, Number reading) {
        super(nameAndTags);
        try {
            this.reading = KafkaMetricsBatch.Preconditions.checkNumeric(reading);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid single-gauge measurement metric=" + nameAndTags, e);
        }
    }

    SimpleMeasurement(String name, Number reading) {
        super(name);
        try {
            this.reading = KafkaMetricsBatch.Preconditions.checkNumeric(reading);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid single-gauge measurement metric=" + name, e);
        }
    }

    @Override
    public Measure asMeasure(List<Tag> staticTags) {
        Measure measure = new Measure(name, reading.doubleValue());
        staticTags.forEach(measure::addTag);
        tags.forEach(measure::addTag);
        return measure;
    }
}

