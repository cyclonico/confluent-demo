package com.lappis.demo.producer;

import java.io.BufferedReader;
import java.io.IOException;
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
 * Hello world!
 *
 */
public class App 
{
    
    public static void main( String[] args )
    {

        Properties props = setProperties();
        String topic = "petFacts";
        String key = "catFact";

        try (Producer<String,GenericRecord> producer = new KafkaProducer<>(props)) {

            Schema userSchema = loadSchema("schema-petFacts-value-v2.avsc");

            //while (true) {

                String catFact = fetchCatFact();
                GenericRecord petFactRecord = buildRecord(userSchema,
                    "cat",
                    catFact.toLowerCase().contains("cats") ? "factual" : "anecdotal",
                    catFact);

                System.out.printf("producing: %s = %s in topic %s\n",key,petFactRecord.toString(),topic);
                producer.send(new ProducerRecord<String,GenericRecord>(topic,key,petFactRecord));

            //}

            producer.flush();
            producer.close();
        } catch (SerializationException e) {
            e.printStackTrace();
        }
    }

    private static GenericRecord buildRecord(Schema schema, String species, String type, String fact) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("petSpecies",species);
        record.put("factType",type);
        record.put("fact",fact);
        return record;
    }

    private static Schema loadSchema(String fName) {

        String schemaString = null;
        FileInputStream inputStream;
        try {

            inputStream = new FileInputStream(fName);
            schemaString = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                  .lines()
                  .collect(Collectors.joining("\n"));
            inputStream.close();

        } catch (FileNotFoundException e) {

            e.printStackTrace();
            System.exit(-1);

        } catch (IOException e) {
            
            e.printStackTrace();
            System.exit(-1);

        }

        return new Schema.Parser().parse(schemaString);
    }

    private static Properties setProperties() {
        Properties props = new Properties();
        
        props.put("bootstrap.servers","pkc-3w22w.us-central1.gcp.confluent.cloud:9092");
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule   required username='RBT6FFOT2A54MVHV'   password='/1GjfCoZyWpVfCLylly7O6eqIj6MD+KHrl0DgNNm5TRShT6yc9ifljNCKDGKDlrI';");
        props.put("sasl.mechanism","PLAIN");

        props.put("acks","all");

        props.put("schema.registry.url","https://psrc-j98yq.us-central1.gcp.confluent.cloud");
        props.put("basic.auth.credentials.source","USER_INFO");
        props.put("basic.auth.user.info","DDV7SPEFMJYX3GJM:A9koXy2OxTavTq68egpF9N/2KSy1GhrW61K+aXzjRhZ9M4m/gbCLkKtJJHaM8V1/");

        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");

        return props;
    }

    private static String fetchCatFact() {

        String result = null;
    
        try {

            URL url = new URL("https://meowfacts.herokuapp.com/");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder stringBuilder = new StringBuilder();
            br.lines().forEach( line -> stringBuilder.append(line) );
            JSONObject jsonResponse = new JSONObject(stringBuilder.toString());
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

    private static String fetchDogFact() {

        String result = null;
    
        try {

            URL url = new URL("https://dog-facts-api.herokuapp.com/api/v1/resources/dogs?number=1");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder stringBuilder = new StringBuilder("{ \"data\" : ");
            br.lines().forEach( line -> stringBuilder.append(line) );
            stringBuilder.append("}");
            JSONObject jsonResponse = new JSONObject(stringBuilder.toString());
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
