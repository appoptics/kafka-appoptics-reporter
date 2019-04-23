package com.appoptics.integrations.kafka.broker;

import com.appoptics.metrics.client.Sanitizer;
import com.appoptics.metrics.client.Tag;
import com.yammer.metrics.core.MetricName;

import java.util.*;

public class NameAndTags {
    private static final String separator = ".";
    private final String name;
    private final String suffix;
    private final List<Tag> tags;

    // memoize
    private String _name = null;

    NameAndTags(String name) {
        this.name = name;
        this.suffix = null;
        this.tags = Collections.emptyList();
    }

    NameAndTags(MetricName metricName) {
        name = parseName(metricName);
        tags = parseTags(metricName);
        suffix = null;
    }

    private NameAndTags(String name, String suffix, List<Tag> tags) {
        this.name = name;
        this.suffix = suffix;
        this.tags = tags;
    }

    NameAndTags withSuffix(String suffix) {
        return new NameAndTags(name, Sanitizer.METRIC_NAME_SANITIZER.apply(suffix), tags);
    }

    String getName() {
        if (_name == null) {
            StringBuilder builder = new StringBuilder();
            builder.append(name);
            if (notEmpty(suffix)) {
                builder.append(separator).append(suffix);
            }
            _name = builder.toString();
        }
        return _name;
    }

    List<Tag> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NameAndTags that = (NameAndTags) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(suffix, that.suffix) &&
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, suffix, tags);
    }

    @Override
    public String toString() {
        return "NameAndTags{" +
                "name='" + name + '\'' +
                ", suffix='" + suffix + '\'' +
                ", tags=" + tags +
                '}';
    }

    private static boolean notEmpty(String s) {
        return s != null && s.length() != 0;
    }

    private static String parseName(MetricName metricName) {
        return Sanitizer.METRIC_NAME_SANITIZER.apply(
                metricName.getGroup() + separator +
                        metricName.getType() + separator +
                        metricName.getName());
    }

    private static List<Tag> parseTags(MetricName metricName) {
        List<Tag> tempList = new ArrayList<>();

        if (metricName.hasScope()) {
            String scope = metricName.getScope();
            String[] parts = scope.split("\\.");
            if (parts.length % 2 == 0) {
                for (int i = 0; i < parts.length; i += 2) {
                    String name = Sanitizer.TAG_NAME_SANITIZER.apply(parts[i]);
                    String value = Sanitizer.TAG_VALUE_SANITIZER.apply(parts[i+1]);
                    tempList.add(new Tag(name, value));
                }
            } else {
                tempList.add(new Tag("scope", Sanitizer.TAG_VALUE_SANITIZER.apply(scope)));
            }
        }

        return Collections.unmodifiableList(tempList);
    }
}
