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

import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.common.KafkaException;

/**
 * polls data from the topic fed by the petFactual stream and displays it in the console
 */
public class FactualConsumer 
{
    public static void main( String[] args )
    {
        // loading properties
        Properties props = loadProperties("java.config");
        // the topic was named automatically using 'CREATE STREAM AS SELECT...'
        String topic = "pksqlc-1ynr5PETFACTUALSTREAM";

        boolean keepConsuming = true;

        // Instantiating the consumer
        try (Consumer<String,GenericRecord> consumer = new KafkaConsumer<>(props)) {
            // subscribing to the topic
            consumer.subscribe(Collections.singleton(topic));
            // polling loop
            while (keepConsuming) {
                // fetching records
                ConsumerRecords<String,GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,GenericRecord> record : records) {
                    // displaying all fetched records
                    System.out.printf("offset=%d, partition=%d, key=%s, value=%s\n",record.offset(),record.partition(), record.key(),record.value());
                }
            }
        } catch (KafkaException e) {
            e.printStackTrace();
        }

    }

    /**
     * Loads properties for the consumer and schema registry from given file
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
