# HTTP Sink Connect
Projeto base para um connector kafka http sink
##created by Fernando Lopes##

```shell
/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic fefe-teste --bootstrap-server localhost:9092

/opt/bitnami/kafka/bin/kafka-console-producer.sh --topic fefe-teste --bootstrap-server localhost:9092

/opt/bitnami/kafka/bin/connect-standalone.sh /opt/bitnami/kafka/config/connect-standalone.properties /opt/bitnami/kafka/config/connect-console-sink.properties

/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fefe-teste --from-beginning
```