package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.Measure;
import com.appoptics.metrics.client.Sanitizer;
import com.appoptics.metrics.client.Tag;

import java.util.Collections;
import java.util.List;


/**
 * Represents a Librato measurement
 */
public abstract class Measurement {
    final String name;
    final List<Tag> tags;

    public Measurement(NameAndTags nameAndTags) {
        try {
            KafkaMetricsBatch.Preconditions.checkNotNull(nameAndTags);
            this.name = nameAndTags.getName();
            this.tags = nameAndTags.getTags();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid single-gauge measurement metric=" + nameAndTags, e);
        }
    }

    public Measurement(String name) {
        KafkaMetricsBatch.Preconditions.checkNotNull(name);
        this.name = Sanitizer.METRIC_NAME_SANITIZER.apply(name);
        this.tags = Collections.emptyList();
    }

    public abstract Measure asMeasure(Tag... staticTags);
}

