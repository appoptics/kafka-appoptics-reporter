package com.appoptics.integrations.kafka.broker;

import com.librato.metrics.client.LibratoClientBuilder;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class KafkaAppopticsReporter implements KafkaMetricsReporter, KafkaAppopticsReporterMBean {

    private final Logger LOG = LoggerFactory.getLogger(KafkaAppopticsReporter.class);
    private Reporter.Builder reporterBuilder;
    private Reporter reporter;

    @Override
    public synchronized void init(VerifiableProperties props) {
        String apiUrl = props.getString("appoptics.url");
        if (apiUrl == null) {
            LOG.info("no apiURL specified, defaulting to production");
            apiUrl = "https://metrics-api.librato.com/v1/measurements";
        }

        String username = props.getString("appoptics.username");
        String token = props.getString("appoptics.token");
        String source = props.getString("appoptics.agent.identifier");
        LibratoClientBuilder libratoClientBuilder = new LibratoClientBuilder(username, token);
        libratoClientBuilder.setURI(apiUrl);
        reporterBuilder = Reporter.builder(source, libratoClientBuilder.build());

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