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
 * pet fact producer app
 * 
 */
public class ProducerDemo 
{
    
    public static void main( String[] args )
    {
        // loading properties, setting topic, and key for produced events
        Properties props = loadProperties("java.config");
        String topic = "petFacts";
        String key = "catFact";

        // creating producer
        try (Producer<String,GenericRecord> producer = new KafkaProducer<>(props)) {

            // reading schema
            Schema userSchema = loadSchema("schema-petFacts-value-v2.avsc");

            // production loop
            boolean keepProducing = true;
            while (keepProducing) {

                // generic record to hold the avro data
                GenericRecord petFactRecord;

                // toss a coin to choose between cat and dog fact
                if (Math.floor(Math.random()*2) <1) {
                    // fetch fact
                    String catFact = fetchCatFact();
                    // populate record
                    petFactRecord = buildRecord(userSchema,
                        "cat",
                        catFact.toLowerCase().contains("cats") ? "factual" : "anecdotal",
                        catFact);
                } else {
                    //fetch fact
                    String dogFact = fetchDogFact();
                    // populate record
                    petFactRecord = buildRecord(userSchema,
                        "dog",
                        dogFact.toLowerCase().contains("dogs") ? "factual" : "anecdotal",
                        dogFact);
                }

                System.out.printf("producing: %s = %s in topic %s\n",key,petFactRecord.toString(),topic);
                // send record to kafka topic
                producer.send(new ProducerRecord<String,GenericRecord>(topic,key,petFactRecord));

                Thread.sleep(500);

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
     * 
     * @param schema
     * @param species
     * @param type
     * @param fact
     * @return
     */
    private static GenericRecord buildRecord(Schema schema, String species, String type, String fact) {
        // create generic record based on schema
        GenericData.Record record = new GenericData.Record(schema);
        // populate key value pairs
        record.put("petSpecies",species);
        record.put("factType",type);
        record.put("fact",fact);
        return record;
    }

    /**
     * 
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

        // connection settings to access the Kafka brokers
        //props.put("bootstrap.servers","pkc-3w22w.us-central1.gcp.confluent.cloud:9092");
        //props.put("security.protocol","SASL_SSL");
        //props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule   required username='RBT6FFOT2A54MVHV'   password='/1GjfCoZyWpVfCLylly7O6eqIj6MD+KHrl0DgNNm5TRShT6yc9ifljNCKDGKDlrI';");
        //props.put("sasl.mechanism","PLAIN");
        // all replicas must be acknowledged
        //props.put("acks","all");
        // connection settings to access the schema registry
        //props.put("schema.registry.url","https://psrc-j98yq.us-central1.gcp.confluent.cloud");
        //props.put("basic.auth.credentials.source","USER_INFO");
        //props.put("basic.auth.user.info","DDV7SPEFMJYX3GJM:A9koXy2OxTavTq68egpF9N/2KSy1GhrW61K+aXzjRhZ9M4m/gbCLkKtJJHaM8V1/");

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
