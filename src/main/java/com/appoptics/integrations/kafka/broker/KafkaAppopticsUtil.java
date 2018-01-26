package com.appoptics.integrations.kafka.broker;

import com.yammer.metrics.core.MetricName;

/**
 * Keeps general Librato utilities out of the way
 */
public class KafkaAppopticsUtil {
    private KafkaAppopticsUtil() {
        // do not instantiate; static utility class
    }

    public static String checkPrefix(String prefix) {
        if ("".equals(prefix)) {
            throw new IllegalArgumentException("Prefix may either be null or a non-empty string");
        }
        return prefix;
    }

    /**
     * turn a MetricName into a Librato-able string key
     */
    public static String nameToString(MetricName name) {
        final String separator = ".";
        final StringBuilder builder = new StringBuilder();
        builder
                .append(name.getGroup()).append(separator)
                .append(name.getType()).append(separator)
                .append(name.getName());

        if (name.hasScope()) {
            builder.append(separator).append(name.getScope());
        }

        return builder.toString();
    }
}
