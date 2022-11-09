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
 * fetches facts from the dog facts API, and streams the data to the dogFacts topic on the Confluent cloud
 */
public class DogProducer 
{
    
    public static void main( String[] args )
    {
        // loading properties, setting topic, and key for produced events
        Properties props = loadProperties("java.config");
        String topic = "dogFacts";
        String key = "dogFact";

        // creating producer
        try (Producer<String,GenericRecord> producer = new KafkaProducer<>(props)) {

            // reading schema
            Schema userSchema = loadSchema("schema-dogFacts-value-v2.avsc");

            // production loop
            boolean keepProducing = true;
            while (keepProducing) {

                // generic record to hold the avro data
                GenericRecord petFactRecord;

                // fetch fact
                String dogFact = fetchDogFact();
                // populate record
                petFactRecord = buildRecord(userSchema,
                    dogFact.toLowerCase().contains("dogs") ? "factual" : "anecdotal",
                    dogFact);

                System.out.printf("producing: %s = %s in topic %s\n",key,petFactRecord.toString(),topic);
                // send record to kafka topic
                producer.send(new ProducerRecord<String,GenericRecord>(topic,key,petFactRecord));

                Thread.sleep(1);

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
     * loads the properties for the producer and schema registry from the given file name
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

        props.put("acks","all");
        props.put("linger.ms", 0);

        // key and value serialisers
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");

        return props;
    }

    /**
     * fetchs and returns a dog fact from REST API
     * @return dog fact
     */
    private static String fetchDogFact() {

        String result = null;
    
        try {
            // connect and send request
            URL url = new URL("https://dog-facts-api.herokuapp.com/api/v1/resources/dogs?number=1");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            // process the result
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            // the result is stored in a JSON array named data, this is to be able to process the result without error
            StringBuilder stringBuilder = new StringBuilder("{ \"data\" : ");
            br.lines().forEach( line -> stringBuilder.append(line) );
            stringBuilder.append("}");
            JSONObject jsonResponse = new JSONObject(stringBuilder.toString());
            // the fact is extraceted from the JSON data: { "data": [ { "fact" : "dog fact" } ] }
            result = ((JSONObject)(jsonResponse.getJSONArray("data").get(0))).getString("fact");

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

}
