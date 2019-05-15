package com.appoptics.integrations.kafka.broker;


import com.appoptics.metrics.client.Measure;
import com.appoptics.metrics.client.Tag;

import java.util.List;

/**
 * A class for representing a gauge reading that might come from multiple samples
 * <p/>
 * See http://dev.librato.com/v1/post/metrics for why some fields are optional
 */
public class MultiSampleMeasurement extends Measurement {
    private final Long count;
    private final Number sum;
    private final Number max;
    private final Number min;

    public MultiSampleMeasurement(NameAndTags nameAndTags,
                                  Long count,
                                  Number sum,
                                  Number max,
                                  Number min) {
        super(nameAndTags);
        try {
            if (count == null || count == 0) {
                throw new IllegalArgumentException("The Librato API requires the count to be > 0 for complex metrics. See http://dev.librato.com/v1/post/metrics");
            }
            this.count = count;
            this.sum = KafkaMetricsBatch.Preconditions.checkNumeric(sum);
            this.max = KafkaMetricsBatch.Preconditions.checkNumeric(max);
            this.min = KafkaMetricsBatch.Preconditions.checkNumeric(min);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid multi-sample gauge measurement metric=" + nameAndTags, e);
        }
    }

    @Override
    public Measure asMeasure(List<Tag> staticTags) {
        Measure measure = new Measure(
                name,
                sum.doubleValue(),
                count,
                min.doubleValue(),
                max.doubleValue());
        staticTags.forEach(measure::addTag);
        tags.forEach(measure::addTag);
        return measure;
    }}

