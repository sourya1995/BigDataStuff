package com.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class AvroConsumer {
    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String TOPIC = "KafkaPractice-topic";

    private static Consumer<Long, KafkaPractice> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaPracticeAvroConsumer");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //Schema registry location.
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081"); //<----- Run Schema Registry on 8081

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        //Use Kafka Avro Deserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());  //<----------------------
        //Use Specific Record or else you get Avro GenericRecord.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        return new KafkaConsumer<>(props);
    }

    public static void main(String... args) {
        final Consumer<Long, KafkaPractice> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));
        IntStream.range(1, 100).forEach(index -> {
            final ConsumerRecords<Long, KafkaPractice> records =
                    consumer.poll(100);
            if (records.count() == 0) {
                System.out.println("None found");
            } else records.forEach(record -> {
                System.out.printf("%s %d %d %s \n", record.topic(),
                        record.partition(), record.offset(), record.value());
            });
        });
    }
}
