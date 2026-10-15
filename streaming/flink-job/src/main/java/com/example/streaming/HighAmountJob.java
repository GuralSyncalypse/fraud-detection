package com.example.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;

public final class HighAmountJob {
    private HighAmountJob() {
    }

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "stream-kafka:19092");
        String transactionsTopic = env("TRANSACTIONS_TOPIC", "flink-transactions");
        String alertsTopic = env("ALERTS_TOPIC", "flink-alerts");
        double threshold = Double.parseDouble(env("HIGH_AMOUNT_THRESHOLD", "1000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L, CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(transactionsTopic)
                .setGroupId("fraud-detection-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(alertsTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka transaction source")
                .flatMap(new HighAmountFlagger(threshold))
                .name("High amount rule")
                .uid("high-amount-rule")
                .sinkTo(sink)
                .name("Kafka fraud alerts sink")
                .uid("fraud-alerts-sink");

        env.execute("Fraud detection streaming add-on");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static final class HighAmountFlagger extends RichFlatMapFunction<String, String> {
        private final double threshold;
        private transient ObjectMapper mapper;

        private HighAmountFlagger(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void flatMap(String value, Collector<String> output) throws IOException {
            JsonNode transaction;
            try {
                transaction = mapper.readTree(value);
            } catch (IOException exception) {
                System.err.println("Skipping invalid transaction JSON: " + exception.getMessage());
                return;
            }

            double amount = transaction.path("amount").asDouble(Double.NaN);
            if (!Double.isFinite(amount) || amount <= threshold) {
                return;
            }

            ObjectNode alert = mapper.createObjectNode();
            alert.put("transactionId", transaction.path("transactionId").asText("unknown"));
            alert.put("accountId", transaction.path("accountId").asText("unknown"));
            alert.put("amount", amount);
            alert.put("currency", transaction.path("currency").asText("USD"));
            alert.put("threshold", threshold);
            alert.put("status", "NEEDS_REVIEW");
            alert.put("reason", "HIGH_AMOUNT");
            alert.put("detectedAt", Instant.now().toString());
            output.collect(mapper.writeValueAsString(alert));
        }
    }
}
