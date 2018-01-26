package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.Sanitizer;
import com.librato.metrics.client.Versions;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class AppopticsBatch {
    public static final int DEFAULT_BATCH_SIZE = 500;
    private static final Logger LOG = LoggerFactory.getLogger(AppopticsBatch.class);
    private static final String LIB_VERSION = Versions.getVersion("META-INF/maven/com.librato.metrics/librato-java/pom.properties", AppopticsBatch.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final List<Measurement> measurements = new ArrayList<>();
    private final int postBatchSize;
    private final Sanitizer sanitizer;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final String userAgent;

    /**
     * Constructor
     *
     * @param postBatchSize size at which to break up the batch
     * @param sanitizer the sanitizer to use for cleaning up metric names to comply with librato api requirements
     * @param timeout time allowed for post
     * @param timeoutUnit unit for timeout
     * @param agentIdentifier a string that identifies the poster (such as the name of a library/program using librato-java)
     */
    public AppopticsBatch(int postBatchSize,
                          final Sanitizer sanitizer,
                          long timeout,
                          TimeUnit timeoutUnit,
                          String agentIdentifier) {
        this.postBatchSize = postBatchSize;
        this.sanitizer = name -> Sanitizer.LAST_PASS.apply(sanitizer.apply(name));
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.userAgent = String.format("%s librato-java/%s", agentIdentifier, LIB_VERSION);
    }

    /**
     * for advanced measurement fu
     */
    public void addMeasurement(Measurement measurement) {
        measurements.add(measurement);
    }

    public void addCounterMeasurement(String name, Long value) {
        addMeasurement(new CounterMeasurement(name, value));
    }

    public void addGaugeMeasurement(String name, Number value) {
        addMeasurement(new SingleValueGaugeMeasurement(name, value));
    }
}

