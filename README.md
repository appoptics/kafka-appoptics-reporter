# Kafka AppOptics Reporter

Reports Kafka metrics to [AppOptics](https://appoptics.com).

[[https://github.com/appoptics/kafka-appoptics-reporter/blob/master/kafka_dashboard.png|alt=Kafka Dashboard]]

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
```
