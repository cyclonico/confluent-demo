package com.lappis.demo.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.stream.Collectors;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.json.JSONObject;

import org.json.JSONException;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Dog fact producer app
 * fetches facts from the cat facts API, and streams the data to the dogFacts topic on the Confluent cloud
 */
public class CatProducer 
{

    private static int LINGER_MS = 0;
    private static int BATCH_SIZE = 16384;
    private static String ACKS = "all";
    private static String KEY = null;
    private static String SCHEMA_PATH = "schema-catFacts-value-v2.avsc";
    private static String PROPS_PATH = "java.config";
    private static int SLEEP_MS = 500;
    
    public static void main( String[] args )
    {
        // loading properties, setting topic, and key for produced events
        Properties props = loadProperties(PROPS_PATH);
        String topic = "catFacts";

        // creating producer
        try (Producer<String,GenericRecord> producer = new KafkaProducer<>(props)) {

            // reading schema
            Schema avroSchema = loadSchema(SCHEMA_PATH);

            // production loop
            boolean keepProducing = true;
            while (keepProducing) {

                // generic record to hold the avro data
                GenericRecord petFactRecord;

                // fetch fact
                String catFact = fetchCatFact();
                // populate record
                petFactRecord = buildRecord(avroSchema,
                    catFact.toLowerCase().contains("cats") ? "factual" : "anecdotal",
                    catFact);

                System.out.printf("producing in topic %s: key=%s, value=%s\n",topic,KEY,truncated(petFactRecord.toString(),80));
                // send record to kafka topic
                if (KEY == null) {
                    producer.send(new ProducerRecord<String,GenericRecord>(topic,petFactRecord),(recordMetadata, exception) -> {
                        if (exception == null) {
                            System.out.println("Fact " + recordMetadata.toString() +
                                    " written to partition " + recordMetadata.partition() +
                                    " of topic " + recordMetadata.topic() +
                                    " at offset " + recordMetadata.offset() +
                                        " timestamp " + recordMetadata.timestamp());
                        } else {
                            System.err.println("An error occurred");
                            exception.printStackTrace(System.err);
                        }
                    });
                } else {
                    producer.send(new ProducerRecord<String,GenericRecord>(topic,KEY,petFactRecord),(recordMetadata, exception) -> {
                        if (exception == null) {
                            System.out.println("Fact " + recordMetadata.toString() +
                                    " written to partition " + recordMetadata.partition() +
                                    " of topic " + recordMetadata.topic() +
                                    " at offset " + recordMetadata.offset() +
                                        " timestamp " + recordMetadata.timestamp());
                        } else {
                            System.err.println("An error occurred");
                            exception.printStackTrace(System.err);
                        }
                    });
                }

                Thread.sleep(SLEEP_MS);

                //keepProducing = false;
            }

            // flush before closing
            producer.flush();
            
        } catch (SerializationException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * builds a fact record that will compy with the given schema, this is specific to the cat/dog facts
     * @param schema
     * @param species
     * @param type
     * @param fact
     * @return
     */
    private static GenericRecord buildRecord(Schema schema, String type, String fact) {
        // create generic record based on schema
        GenericData.Record record = new GenericData.Record(schema);
        // populate key value pairs
        record.put("factType",type);
        record.put("fact",fact);
        return record;
    }

    /**
     * loads the schema from the given file
     * @param fName
     * @return
     */
    private static Schema loadSchema(String fName) {

        // read avro schema from avsc file 
        String schemaString = null;
        try (FileInputStream inputStream = new FileInputStream(fName);) {
            schemaString = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                  .lines()
                  .collect(Collectors.joining("\n"));
        } catch (FileNotFoundException e) {
            System.out.println("Schema file not found.");
            System.exit(-1);
        } catch (IOException e) {
            System.out.println("Error while reading schema file.");
            System.exit(-1);
        }

        // returns schema parsed from file content
        return new Schema.Parser().parse(schemaString);
    }

    /**
     * load the properties for the producer
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

        props.put("acks",ACKS);
        props.put("batch.size",BATCH_SIZE);
        props.put("linger.ms", LINGER_MS);

        // key and value serialisers
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");

        return props;
    }

    /**
     * fetch and retusn a cat fact from REST API
     * @return cat fact
     */
    private static String fetchCatFact() {

        String result = null;
    
        try {
            // connect to API and send request
            URL url = new URL("https://meowfacts.herokuapp.com/");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            // process the response
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder stringBuilder = new StringBuilder();
            br.lines().forEach( line -> stringBuilder.append(line) );
            JSONObject jsonResponse = new JSONObject(stringBuilder.toString());
            // the result is the first element of the JSON array "data": { "data" : [ "cat fact" ] }
            result = (String)jsonResponse.getJSONArray("data").get(0);

            conn.disconnect();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return result;
    }

    private static String truncated(String text, int length) {
        if (length < 3) length = 3;
        if (text.length() <= length-3) return text;
        return text.substring(0,length-3) + "...";
    }

}
