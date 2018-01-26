package com.appoptics.integrations.kafka.broker;

import java.util.Map;

/**
 * Represents a Librato measurement
 */
public interface Measurement {

    /**
     * @return the name of the measurement
     */
    String getName();

    /**
     * @return a map of metric names to numbers
     */
    Map<String, Number> toMap();
}

