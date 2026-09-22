
package com.recallradar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class RecallRadarApplication {

    public static void main(String[] args) {

        SpringApplication.run(
                RecallRadarApplication.class,
                args
        );
    }
}


// ============================================
// DASHBOARD DATA MODELS
// ============================================

record AlertView(
        String alertId,
        String customerId,
        String productId,
        String batchId,
        String severity,
        String reason,
        String instructions,
        String timestamp
) {
}

record RecallView(
        String recallId,
        String productId,
        String batchId,
        String severity,
        long affectedCustomers,
        long totalAlerts
) {
}

record DashboardSnapshot(
        long totalAlerts,
        long affectedCustomers,
        int activeRecalls,
        List<RecallView> recalls,
        List<AlertView> recentAlerts,
        String lastUpdated
) {
}


// ============================================
// KAFKA DASHBOARD SERVICE
// ============================================

@Component
class DashboardService {

    private static final String ALERT_TOPIC =
            "rr-alerts";

    private final ObjectMapper mapper =
            new ObjectMapper();

    // LinkedHashMap preserves insertion order.
    // All access is synchronized on this service.
    private final Map<String, JsonNode> alerts =
            new LinkedHashMap<>();

    private final AtomicBoolean running =
            new AtomicBoolean(true);

    private volatile KafkaConsumer<String, String> consumer;

    private Thread workerThread;


    @EventListener(ApplicationReadyEvent.class)
    public void start() {

        workerThread = new Thread(
                this::consumeAlerts,
                "recallradar-dashboard-consumer"
        );

        workerThread.setDaemon(true);
        workerThread.start();

        System.out.println(
                "RecallRadar dashboard consumer started."
        );
    }


    private Properties consumerProperties() {

        Properties base =
                KafkaConfig.producerProperties();

        Properties props =
                new Properties();

        // Reuse the existing Confluent Cloud connection.
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                base.getProperty("bootstrap.servers")
        );

        props.put(
                "security.protocol",
                base.getProperty("security.protocol")
        );

        props.put(
                "sasl.mechanism",
                base.getProperty("sasl.mechanism")
        );

        props.put(
                "sasl.jaas.config",
                base.getProperty("sasl.jaas.config")
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        // Unique consumer group for each dashboard run.
        // Allows the demo to replay existing alert records.
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "recallradar-dashboard-"
                        + UUID.randomUUID()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        return props;
    }


    private void consumeAlerts() {

        try (
                KafkaConsumer<String, String> kafka =
                        new KafkaConsumer<>(
                                consumerProperties()
                        )
        ) {

            consumer = kafka;

            kafka.subscribe(
                    List.of(ALERT_TOPIC)
            );

            System.out.println(
                    "Listening for RecallRadar alerts..."
            );

            while (running.get()) {

                ConsumerRecords<String, String> records =
                        kafka.poll(
                                Duration.ofMillis(1000)
                        );

                for (
                        ConsumerRecord<String, String> record
                        : records
                ) {

                    try {

                        JsonNode alert =
                                mapper.readTree(
                                        record.value()
                                );

                        String alertId =
                                alert.path("alertId").asText();

                        String customerId =
                                alert.path("customerId").asText();

                        if (alertId.isBlank()
                                || customerId.isBlank()) {

                            System.err.println(
                                    "Ignoring invalid alert."
                            );

                            continue;
                        }

                        synchronized (this) {

                            // Avoid counting replayed alerts twice.
                            alerts.putIfAbsent(
                                    alertId,
                                    alert
                            );
                        }

                    } catch (Exception exception) {

                        System.err.println(
                                "Failed to parse alert: "
                                        + exception.getMessage()
                        );
                    }
                }
            }

        } catch (WakeupException exception) {

            if (running.get()) {
                throw exception;
            }

        } catch (Exception exception) {

            System.err.println(
                    "Dashboard consumer failed: "
                            + exception.getMessage()
            );

        } finally {

            consumer = null;
        }
    }


    // ========================================
    // BUILD DASHBOARD SNAPSHOT
    // ========================================

    public synchronized DashboardSnapshot snapshot() {

        Map<String, List<JsonNode>> recallGroups =
                new LinkedHashMap<>();

        Set<String> uniqueCustomers =
                new HashSet<>();

        List<AlertView> recentAlerts =
                new ArrayList<>();

        for (JsonNode alert : alerts.values()) {

            String customerId =
                    alert.path("customerId").asText();

            String recallId =
                    alert.path("recallId").asText();

            uniqueCustomers.add(customerId);

            recallGroups.computeIfAbsent(
                    recallId,
                    ignored -> new ArrayList<>()
            ).add(alert);

            recentAlerts.add(
                    new AlertView(
                            alert.path("alertId").asText(),
                            customerId,
                            alert.path("productId").asText(),
                            alert.path("batchId").asText(),
                            alert.path("severity").asText(),
                            alert.path("reason").asText(),
                            alert.path("instructions").asText(),
                            alert.path("timestamp").asText()
                    )
            );
        }

        List<RecallView> recalls =
                new ArrayList<>();

        for (var entry : recallGroups.entrySet()) {

            List<JsonNode> group =
                    entry.getValue();

            JsonNode first =
                    group.get(0);

            long affectedCustomers =
                    group.stream()
                            .map(a ->
                                    a.path("customerId").asText()
                            )
                            .distinct()
                            .count();

            recalls.add(
                    new RecallView(
                            entry.getKey(),
                            first.path("productId").asText(),
                            first.path("batchId").asText(),
                            first.path("severity").asText(),
                            affectedCustomers,
                            group.size()
                    )
            );
        }

        // Display newest alerts first.
        Collections.reverse(recentAlerts);

        // Keep the browser response small.
        List<AlertView> latest =
                recentAlerts.stream()
                        .limit(15)
                        .toList();

        return new DashboardSnapshot(
                alerts.size(),
                uniqueCustomers.size(),
                recalls.size(),
                recalls,
                latest,
                Instant.now().toString()
        );
    }


    @PreDestroy
    public void shutdown() {

        running.set(false);

        KafkaConsumer<String, String> kafka =
                consumer;

        if (kafka != null) {
            kafka.wakeup();
        }

        if (workerThread != null) {

            try {
                workerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


// ============================================
// REST CONTROLLER
// ============================================

@RestController
class DashboardController {

    private final DashboardService service;

    public DashboardController(
            DashboardService service
    ) {

        this.service = service;
    }


    @GetMapping("/api/dashboard")
    public DashboardSnapshot dashboard() {

        return service.snapshot();
    }
}