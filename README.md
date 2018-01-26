# Kafka AppOptics Reporter

Reports Kafka metrics to Librato.

# Usage

* Add jar to Kafka's lib directory
* Add the following to Kafka's server.properties

```
kafka.metrics.reporters=com.appoptics.integrations.kafka.broker.KafkaAppopticsReporter

# Configure reporting metrics to Librato
appoptics.url=[api-url]
appoptics.username=[username]
appoptics.token=[api-token]
appoptics.agent.identifier=[hostname]

```
