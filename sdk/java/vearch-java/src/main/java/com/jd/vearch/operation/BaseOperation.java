package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jd.vearch.exception.VearchException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public abstract class BaseOperation {

    private String baseUrl;
    private String userName = "root";
    private String token = "token";

    public BaseOperation(){

    }

    public BaseOperation(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public BaseOperation(String baseUrl, String userName, String token){
        this.baseUrl = baseUrl;
        this.userName = userName;
        this.token = token;
    }


    public String sendGetRequest(String endpoint) {
        return this.sendGetRequest(endpoint, null);
    }
    public String sendGetRequest(String endpoint, Map<String, Object> params) {
        StringBuilder urlBuilder = new StringBuilder(baseUrl + endpoint);
        if (params != null && !params.isEmpty()) {
            urlBuilder.append("?");
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                urlBuilder.append(key).append("=").append(value).append("&");
            }
            urlBuilder.deleteCharAt(urlBuilder.length() - 1);
        }
        try {
            URL url = new URL(urlBuilder.toString());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            if(userName == null || userName.isEmpty() || token == null || token.isEmpty()){
                throw new VearchException("auth header is empty");
            }
            String auth = userName + ":" + token;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                return response.toString();
            } else {
                throw new IOException("Failed to send GET request. Response code: " + responseCode);
            }
        }catch (IOException e) {
            throw new VearchException("Failed to send GET request. Response code: " + e.getMessage());
        }
    }
    public String sendDeleteRequest(String endpoint) {
        return this.sendDeleteRequest(endpoint, null);
    }

    public String sendDeleteRequest(String endpoint, Map<String, String> params){
        StringBuilder urlBuilder = new StringBuilder(baseUrl + endpoint);
        if (params != null && !params.isEmpty()) {
            urlBuilder.append("?");
            for (Map.Entry<String, String> entry : params.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                urlBuilder.append(key).append("=").append(value).append("&");
            }
            urlBuilder.deleteCharAt(urlBuilder.length() - 1);
        }
        try {
            URL url = new URL(urlBuilder.toString());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            if(userName == null || userName.isEmpty() || token == null || token.isEmpty()){
                throw new VearchException("auth header is empty");
            }
            String auth = userName + ":" + token;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                return response.toString();
            } else {
                throw new VearchException("Failed to send GET request. Response code: " + responseCode);
            }
        }catch (Exception e){
            throw new VearchException("Failed to send GET request. Response code: " + e.getMessage());
        }
    }
    public String sendPostRequest(String endpoint, Map<String,Object> body) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return this.sendPostRequest(endpoint, objectMapper.writeValueAsString(body));
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    public String sendPostRequest(String endpoint, String body) {
        StringBuilder urlBuilder = new StringBuilder(baseUrl + endpoint);
        StringBuilder response = new StringBuilder();
        try {
            URL url = new URL(urlBuilder.toString());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            if(userName == null || userName.isEmpty() || token == null || token.isEmpty()){
                throw new VearchException("auth header is empty");
            }
            String auth = userName + ":" + token;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            connection.setDoOutput(true);
            connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

            // 设置连接超时时间为30秒
            connection.setConnectTimeout(30000);
            // 设置读取超时时间为30秒
            connection.setReadTimeout(30000);

            int responseCode = connection.getResponseCode();
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                return response.toString();
            } else {
//                throw new IOException("Failed to send POST request. Response code: " + responseCode);
                System.out.println(response);
                return response.toString();
            }
        }catch (Exception e){
            System.out.println(e+"response====="+response.toString());
            throw new VearchException("Failed to send POST request. Response code: " + e.getMessage());
        }
    }


    public String sendPutRequest(String endpoint, Map<String,Object> body) {
        return this.sendPutRequest(endpoint, JSON.toJSONString(body));
    }

    public String sendPutRequest(String endpoint, String body) {
        System.out.println(body);
        try {
            URL url = new URL(baseUrl + endpoint);
            System.out.println(url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/json");
            if(userName == null || userName.isEmpty() || token == null || token.isEmpty()){
                throw new VearchException("auth header is empty");
            }
            String auth = userName + ":" + token;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            connection.setDoOutput(true);
            connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                return response.toString();
            } else {
                System.out.println("Failed to send POST request. Response code: " + connection.getResponseMessage());
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                System.out.println(response);
                throw new VearchException("Failed to send POST request. Response code: " +responseCode);
            }
        } catch (VearchException e){
            throw e;
        }  catch (Exception e){
            throw new VearchException("Failed to send POST request. Response code: " + e.getMessage());
        }
    }

}