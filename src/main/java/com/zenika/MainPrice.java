package com.zenika;

import com.zenika.avro.price.Price;
import com.zenika.avro.price.PriceKey;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class MainPrice {

    public static void main(String[] args) {

        // configure client
        final Properties produceProperties = configureKafkaClient();

        // start
        final Instant start = Instant.now();


        try (KafkaProducer<Object, Object> producer = new KafkaProducer(produceProperties)) {

            final PriceKey priceKey = PriceKey.newBuilder().setProductIdentifier("").build();
            final Price price = Price.newBuilder().setPrice(0).build();

            for (int i = 0; i < 1_000_000; i++) {
                // Create the ProducerRecord with the Avro objects and send them
                priceKey.setProductIdentifier(String.valueOf(i));
                price.setPrice(Double.valueOf(i));
                final ProducerRecord<Object, Object> avroRecord = new ProducerRecord("price_in_avro", priceKey, price);
                producer.send(avroRecord);
            }

        }
        final Instant end = Instant.now();
        System.out.print("Finish in : " + Duration.between(start, end).toMillis());
    }

    private static Properties configureKafkaClient() {

        Properties produceProperties = new Properties();

        produceProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
        produceProperties.put("schema.registry.url", "http://127.0.0.1:8081");

        produceProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        produceProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        return produceProperties;
    }
}
