package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.*;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class KafkaAppopticsReporter implements KafkaMetricsReporter, KafkaAppopticsReporterMBean {

    private final Logger LOG = LoggerFactory.getLogger(KafkaAppopticsReporter.class);
    private Reporter reporter;

    private static final String URL = "appoptics.url";
    private static final String TOKEN = "appoptics.token";
    private static final String AGENT_IDENTIFIER = "appoptics.agent.identifier";
    private static final String TAGS = "appoptics.tags";
    private static final String DEFAULT_URL = "https://api.appoptics.com/v1/measurements";

    @Override
    public void init(VerifiableProperties props) {
        String apiUrl = props.getString(URL, DEFAULT_URL);

        String token = props.getString(TOKEN);
        List<Tag> tags = new ArrayList<>();

        String source = props.getString(AGENT_IDENTIFIER, "");
        if (!source.isEmpty()) {
            tags.add(new Tag("source", Sanitizer.TAG_VALUE_SANITIZER.apply(source)));
        }

        String customTags = props.getString(TAGS, "");
        if (!customTags.isEmpty()) {
           tags.addAll(TagProcessor.process(customTags));
        }

        int timeout = props.getInt("librato.timeout", 20);

        Set<ExpandedMetric> metrics = new HashSet<>();
        maybeEnableMetric(props, metrics, ExpandedMetric.MEDIAN, true);
        maybeEnableMetric(props, metrics, ExpandedMetric.PCT_75, false);
        maybeEnableMetric(props, metrics, ExpandedMetric.PCT_95, true);
        maybeEnableMetric(props, metrics, ExpandedMetric.PCT_98, false);
        maybeEnableMetric(props, metrics, ExpandedMetric.PCT_99, true);
        maybeEnableMetric(props, metrics, ExpandedMetric.PCT_999, true);

        maybeEnableMetric(props, metrics, ExpandedMetric.COUNT, true);
        maybeEnableMetric(props, metrics, ExpandedMetric.RATE_MEAN, false);
        maybeEnableMetric(props, metrics, ExpandedMetric.RATE_1_MINUTE, true);
        maybeEnableMetric(props, metrics, ExpandedMetric.RATE_5_MINUTE, false);
        maybeEnableMetric(props, metrics, ExpandedMetric.RATE_15_MINUTE, false);

        AppopticsClient client = new AppopticsClientBuilder(token)
                .setURI(apiUrl)
                .setReadTimeout(new Duration(timeout, TimeUnit.SECONDS)).build();
        reporter = new Reporter(client, new ExpandedMetric.ExpandedMetricConfig(metrics), tags);

        if (props.getBoolean("librato.kafka.enable", true)) {
            startReporter(props.getInt("librato.kafka.interval", 30));
        }
    }

    private static void maybeEnableMetric(
            VerifiableProperties props,
            Set<ExpandedMetric> metrics,
            ExpandedMetric metric,
            boolean defaultValue) {

        if (props.getBoolean(metric.buildMetricName("librato.kafka.metrics"), defaultValue)) {
            metrics.add(metric);
        }
    }

    @Override
    public String getMBeanName() {
        return "kafka:type=" + KafkaAppopticsReporter.class.getCanonicalName();
    }

    @Override
    public void startReporter(long interval) {
        if (reporter == null) {
            throw new IllegalStateException("reporter not configured");
        }

        LOG.info("starting appoptics metrics reporter");
        reporter.start(interval, TimeUnit.SECONDS);
    }

    @Override
    public void stopReporter() {
        if (reporter != null) {
            LOG.info("stopping appoptics metrics reporter");
            reporter.shutdown();
            reporter = null;
        }
    }


}