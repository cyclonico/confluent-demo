package com.lappis.demo.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONObject;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try {

            URL url = new URL("https://meowfacts.herokuapp.com/");

            for (int i = 0; i < 10; i++) {

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
    
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }
    
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
    
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                stringBuilder.append(line);
            }

            JSONObject jsonResponse = new JSONObject(stringBuilder.toString());

            System.out.println(jsonResponse.toString());
    
            conn.disconnect();
            }
    
          } catch (MalformedURLException e) {
    
            e.printStackTrace();
    
          } catch (IOException e) {
    
            e.printStackTrace();
    
          }
    }
}
