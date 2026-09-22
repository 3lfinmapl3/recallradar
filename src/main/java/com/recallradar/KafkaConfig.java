
package com.recallradar;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class KafkaConfig {

    private KafkaConfig() {
    }

    public static Properties producerProperties() {

        String bootstrapServers =
                requiredEnv("KAFKA_BOOTSTRAP_SERVERS");

        String apiKey =
                requiredEnv("KAFKA_API_KEY");

        String apiSecret =
                requiredEnv("KAFKA_API_SECRET");

        Properties props = new Properties();

        // Confluent Cloud Kafka endpoint
        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        // Authentication
        props.put(
                "security.protocol",
                "SASL_SSL"
        );

        props.put(
                "sasl.mechanism",
                "PLAIN"
        );

        String jaasConfig =
                "org.apache.kafka.common.security.plain."
                        + "PlainLoginModule required "
                        + "username=\"" + apiKey + "\" "
                        + "password=\"" + apiSecret + "\";";

        props.put(
                "sasl.jaas.config",
                jaasConfig
        );

        // String keys and JSON string values
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // Delivery reliability
        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        props.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        props.put(
                ProducerConfig.RETRIES_CONFIG,
                5
        );

        return props;
    }

    private static String requiredEnv(String name) {

        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Missing environment variable: " + name
            );
        }

        return value;
    }
}