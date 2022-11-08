package com.lappis.demo.consumer;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

// import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.common.KafkaException;

public class ConsumerDemo 
{
    public static void main( String[] args )
    {
        Properties props = loadProperties("java.config");
        String topic = "pksqlc-1ynr5PETFACTUALSTREAM";

        boolean keepConsuming = true;

        try (Consumer<String,GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(topic));
            while (keepConsuming) {
                ConsumerRecords<String,GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",record.offset(),record.key(),record.value());
                }
            }
        } catch (KafkaException e) {
            e.printStackTrace();
        }

    }

    /**
     * 
     * @param fName
     * @return
     */
    private static Properties loadProperties(String fName) {

        Properties props = new Properties();

        try (InputStream inputStream = new FileInputStream(fName)) {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // key and value serialisers
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("group.id", "group1");

        return props;
    }
}
