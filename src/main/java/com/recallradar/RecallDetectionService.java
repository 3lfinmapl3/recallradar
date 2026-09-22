package com.recallradar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class RecallDetectionService {

    private static final String PURCHASES = "rr-purchases";
    private static final String RECALLS = "rr-recalls";
    private static final String ALERTS = "rr-alerts";

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    private final Map<String, List<JsonNode>> purchases =
            new HashMap<>();

    private final Map<String, List<JsonNode>> recalls =
            new HashMap<>();

    private final Set<String> generatedAlerts =
            new HashSet<>();

    private final KafkaProducer<String, String> producer;

    public RecallDetectionService(
            KafkaProducer<String, String> producer) {

        this.producer = producer;
    }

    private static String productBatchKey(JsonNode event) {

        return event.path("productId").asText()
                + ":"
                + event.path("batchId").asText();
    }

    private void processPurchase(JsonNode purchase)
            throws Exception {

        String key = productBatchKey(purchase);

        purchases.computeIfAbsent(
                key, ignored -> new ArrayList<>()
        ).add(purchase);

        for (JsonNode recall :
                recalls.getOrDefault(key, List.of())) {

            generateAlert(purchase, recall);
        }
    }

    private void processRecall(JsonNode recall)
            throws Exception {

        String key = productBatchKey(recall);

        recalls.computeIfAbsent(
                key, ignored -> new ArrayList<>()
        ).add(recall);

        for (JsonNode purchase :
                purchases.getOrDefault(key, List.of())) {

            generateAlert(purchase, recall);
        }
    }

    private void generateAlert(
            JsonNode purchase,
            JsonNode recall
    ) throws Exception {

        String purchaseId =
                purchase.path("purchaseId").asText();

        String recallId =
                recall.path("recallId").asText();

        String alertId =
                purchaseId + ":" + recallId;

        // Avoid duplicate alerts during this service run.
        if (!generatedAlerts.add(alertId)) {
            return;
        }

        ObjectNode alert = MAPPER.createObjectNode();

        alert.put("alertId", alertId);

        alert.put(
                "customerId",
                purchase.path("customerId").asText()
        );

        alert.put(
                "purchaseId",
                purchaseId
        );

        alert.put(
                "recallId",
                recallId
        );

        alert.put(
                "productId",
                purchase.path("productId").asText()
        );

        alert.put(
                "batchId",
                purchase.path("batchId").asText()
        );

        alert.put(
                "severity",
                recall.path("severity").asText()
        );

        alert.put(
                "reason",
                recall.path("reason").asText()
        );

        alert.put(
                "instructions",
                recall.path("instructions").asText()
        );

        alert.put(
                "timestamp",
                Instant.now().toString()
        );

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        ALERTS,
                        purchase.path("customerId").asText(),
                        MAPPER.writeValueAsString(alert)
                );

        producer.send(record).get();

        System.out.printf(
                "RECALL ALERT | Customer: %s | "
                        + "Product: %s | Batch: %s%n",
                purchase.path("customerId").asText(),
                purchase.path("productId").asText(),
                purchase.path("batchId").asText()
        );
    }

    public void run() {

        Properties consumerProperties =
                new Properties();

        consumerProperties.putAll(
                KafkaConfig.producerProperties()
        );

        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "recallradar-detection-demo"
        );

        consumerProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        consumerProperties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(
                    List.of(PURCHASES, RECALLS)
            );

            System.out.println(
                    "RecallRadar detection service started..."
            );

            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record :
                        records) {

                    try {

                        JsonNode event =
                                MAPPER.readTree(record.value());

                        if (PURCHASES.equals(record.topic())) {

                            processPurchase(event);

                        } else if (RECALLS.equals(record.topic())) {

                            processRecall(event);
                        }

                    } catch (Exception exception) {

                        System.err.printf(
                                "Failed to process record: "
                                        + "topic=%s offset=%d error=%s%n",
                                record.topic(),
                                record.offset(),
                                exception.getMessage()
                        );
                    }
                }
            }
        }
    }

    public static void main(String[] args) {

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(
                             KafkaConfig.producerProperties()
                     )) {

            new RecallDetectionService(producer).run();

        } catch (Exception exception) {

            exception.printStackTrace();
        }
    }
}