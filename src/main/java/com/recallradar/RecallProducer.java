
package com.recallradar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class RecallProducer {

    private static final String TOPIC = "rr-recalls";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {

        String recallId =
                UUID.randomUUID().toString();

        String productId = "PB-101";

        String batchId = "BATCH-PB-0917";

        String recallEvent = """
                {
                  "recallId": "%s",
                  "productId": "%s",
                  "batchId": "%s",
                  "severity": "HIGH",
                  "reason": "Potential contamination",
                  "instructions": "Stop use and return the product to the retailer.",
                  "timestamp": "%s"
                }
                """.formatted(
                recallId,
                productId,
                batchId,
                Instant.now()
        );

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(
                             KafkaConfig.producerProperties()
                     )) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            TOPIC,
                            productId + ":" + batchId,
                            recallEvent
                    );

            var metadata = producer.send(record).get();

            System.out.printf(
                    "RECALL PUBLISHED | Topic: %s | Offset: %d%n",
                    metadata.topic(),
                    metadata.offset()
            );

            System.out.println(recallEvent);
        }
    }
}