package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.LibratoClientBuilder;
import com.librato.metrics.client.Tag;
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
    private Reporter.Builder reporterBuilder;
    private Reporter reporter;

    private static final String URL = "appoptics.url";
    private static final String TOKEN = "appoptics.token";
    private static final String AGENT_IDENTIFIER = "appoptics.agent.identifier";
    private static final String TAGS = "appoptics.tags";
    private static final String DEFAULT_URL = "https://api.appoptics.com/v1/measurements";

    @Override
    public synchronized void init(VerifiableProperties props) {
        String apiUrl = props.getString(URL);
        if (apiUrl == null) {
            apiUrl = DEFAULT_URL;
        }

        String token = props.getString(TOKEN);
        List<Tag> tags = new ArrayList<Tag>();

        String source = props.getString(AGENT_IDENTIFIER, "");
        if (!source.isEmpty()) {
            tags.add(new Tag("source", source));
        }

        String customTags = props.getString(TAGS, "");
        if (!customTags.isEmpty()) {
           tags.addAll(TagProcessor.process(customTags));
        }

        LibratoClientBuilder libratoClientBuilder = new LibratoClientBuilder("token", token);
        libratoClientBuilder.setURI(apiUrl);
        reporterBuilder = Reporter.builder(tags, libratoClientBuilder.build());

        Set<Reporter.ExpandedMetric> metrics = new HashSet<>();
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.MEDIAN, true);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.PCT_75, false);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.PCT_95, true);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.PCT_98, false);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.PCT_99, true);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.PCT_999, true);

        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.COUNT, true);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.RATE_MEAN, false);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.RATE_1_MINUTE, true);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.RATE_5_MINUTE, false);
        maybeEnableMetric(props, metrics, Reporter.ExpandedMetric.RATE_15_MINUTE, false);

        reporterBuilder.setTimeout(props.getInt("librato.timeout", 20), TimeUnit.SECONDS)
                .setReportVmMetrics(true)
                .setExpansionConfig(new Reporter.MetricExpansionConfig(metrics));

        if (props.getBoolean("librato.kafka.enable", true)) {
            startReporter(props.getInt("librato.kafka.interval", 30));
        }
    }

    private static void maybeEnableMetric(
            VerifiableProperties props,
            Set<Reporter.ExpandedMetric> metrics,
            Reporter.ExpandedMetric metric,
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
    public synchronized void startReporter(long interval) {
        if (reporterBuilder == null) {
            throw new IllegalStateException("reporter not configured");
        }

        if (reporter == null) {
            LOG.info("starting appoptics metrics reporter");
            reporter = reporterBuilder.build();
            reporter.start(interval, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void stopReporter() {
        if (reporter != null) {
            LOG.info("stopping appoptics metrics reporter");
            reporter.shutdown();
            reporter = null;
        }
    }


}