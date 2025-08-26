package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jd.vearch.client.DatabaseClient;
import com.jd.vearch.exception.VearchException;
import com.jd.vearch.model.Database;
import com.jd.vearch.model.Space;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DatabaseOperation extends BaseOperation implements DatabaseClient {

    public DatabaseOperation() {
        super();
    }

    public DatabaseOperation(String baseUrl) {
        super(baseUrl);
    }

    public DatabaseOperation(String baseUrl, String userName, String token){
        super(baseUrl, userName, token);
    }

    @Override
    public List<Database> getAllDatabases() {
        // 发送HTTP请求到获取集群所有库的端点
        // 处理响应数据，返回库名列表
        String s = sendGetRequest("/dbs");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseArray(response.getString("data"),Database.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public Database createDatabase(String databaseName) {
        // 发送HTTP请求到创建库的端点，传递数据库名称
        // 处理响应结果
        String s = sendPostRequest("/dbs/" + databaseName, (Map<String, Object>) null);
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseObject(response.getString("data"),Database.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public Database viewDatabase(String databaseName) {
        // 发送HTTP请求到查看库的端点，传递数据库名称
        // 处理响应数据，返回库信息的字符串表示
        String s = sendGetRequest("/dbs/"+databaseName);
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseObject(response.getString("data"),Database.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public boolean deleteDatabase(String databaseName) {
        // 发送HTTP请求到删除库的端点，传递数据库名称
        // 处理响应结果
        String s = sendDeleteRequest("/dbs/"+databaseName);
        if(s == null || s.length() == 0){
            return false;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return true;
        }
        return false;
    }

    @Override
    public List<Space> getAllSpaces(String databaseName) {
        // 发送HTTP请求到查看指定库下所有表空间的端点，传递数据库名称
        // 处理响应数据，返回表空间名列表
        String s = sendGetRequest("/dbs/" + databaseName + "/spaces");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseArray(response.getString("data"),Space.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }
}