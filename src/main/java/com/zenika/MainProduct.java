package com.zenika;

import com.zenika.avro.product.Product;
import com.zenika.avro.product.ProductKey;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class MainProduct {

    public static void main(String[] args) {

        final Properties produceProperties = configureKafkaClient();

        final Instant start = Instant.now();
        try (KafkaProducer<Object, Object> producer = new KafkaProducer(produceProperties)) {

            final ProductKey productKey = ProductKey.newBuilder().setProductIdentifier("").build();
            final Product product = Product.newBuilder().setLabel("").setVat("").build();

            for (int i = 0; i < 1_000_000; i++) {
                // Create the ProducerRecord with the Avro objects and send them
                productKey.setProductIdentifier(String.valueOf(i));
                product.setLabel("Product label " + i);
                setRandomVAT(product);

                final ProducerRecord<Object, Object> avroRecord = new ProducerRecord("product_in_avro", productKey, product);
                producer.send(avroRecord);
            }

        }
        final Instant end = Instant.now();
        System.out.print("Finish in : " + Duration.between(start, end).toMillis());
    }

    private static void setRandomVAT(Product product) {
        if (new Random().nextInt(2) == 1) {
            product.setVat("20%");
        } else {
            product.setVat("10%");
        }
    }

    private static Properties configureKafkaClient() {
        Properties produceProperties = new Properties();
        produceProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
        produceProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        produceProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // Configure schema repository server
        produceProperties.put("schema.registry.url", "http://127.0.0.1:8081");
        return produceProperties;
    }
}
