# Kafka AppOptics Reporter

Reports Kafka metrics to [AppOptics](https://appoptics.com).

![Kafka Dashboard](https://github.com/appoptics/kafka-appoptics-reporter/blob/master/kafka_dashboard.png "Kafka Dashboard")

# Supports

* Kafka >= 0.10.0.0

# Building

* `mvn integration-test`
* `mvn package`

# Usage

* Follow build instructions above.
* Add jar from `target/kafka-appoptics-reporter-{version}.jar` to Kafka's lib directory
* Create a *Record Only* API token on the [API Tokens](https://my.appoptics.com/organization/tokens) settings page.
* Add the following to Kafka's server.properties.

```
kafka.metrics.reporters=com.appoptics.integrations.kafka.broker.KafkaAppopticsReporter

# Configure reporting metrics to AppOptics
appoptics.token=[api-token]
appoptics.agent.identifier=[hostname]
appoptics.tags=[tag1name=tagvalue,tag2name=tag2value]
# Tag NAME/value restrictions located: https://docs.appoptics.com/api/#measurement-restrictions
```
