
package com.recallradar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class PurchaseProducer {

    private static final String TOPIC = "rr-purchases";

    private static final String[] PRODUCTS = {
            "PB-101",
            "MILK-202",
            "CHOC-303"
    };

    private static final String[] BATCHES = {
            "BATCH-PB-0917",
            "BATCH-MILK-0920",
            "BATCH-CHOC-0918"
    };

    public static void main(String[] args)
            throws InterruptedException {

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(
                             KafkaConfig.producerProperties()
                     )) {

            System.out.println(
                    "RecallRadar purchase stream started..."
            );

            for (int i = 1; i <= 100; i++) {

                int index = ThreadLocalRandom
                        .current()
                        .nextInt(PRODUCTS.length);

                String productId = PRODUCTS[index];

                String batchId = BATCHES[index];

                String customerId =
                        "CUSTOMER-" + String.format("%03d", i);

                String purchaseId =
                        UUID.randomUUID().toString();

                String storeId =
                        "STORE-" +
                                ThreadLocalRandom.current()
                                        .nextInt(1, 6);

                String event = """
                        {
                          "purchaseId": "%s",
                          "customerId": "%s",
                          "storeId": "%s",
                          "productId": "%s",
                          "batchId": "%s",
                          "timestamp": "%s"
                        }
                        """.formatted(
                        purchaseId,
                        customerId,
                        storeId,
                        productId,
                        batchId,
                        Instant.now()
                );

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                TOPIC,
                                productId + ":" + batchId,
                                event
                        );

                producer.send(record, (metadata, exception) -> {

                    if (exception != null) {

                        System.err.println(
                                "Purchase publish failed: "
                                        + exception.getMessage()
                        );

                    } else {

                        System.out.printf(
                                "Purchase published | "
                                        + "Topic: %s | "
                                        + "Partition: %d | "
                                        + "Offset: %d%n",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    }
                });

                Thread.sleep(500);
            }

            producer.flush();

            System.out.println(
                    "Purchase event generation completed."
            );
        }
    }
}