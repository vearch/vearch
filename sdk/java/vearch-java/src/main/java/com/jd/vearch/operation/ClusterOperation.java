package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jd.vearch.client.ClusterClient;
import com.jd.vearch.exception.VearchException;
import com.jd.vearch.model.cluster.ClusterHealth;
import com.jd.vearch.model.cluster.ClusterStatus;
import com.jd.vearch.model.cluster.PortStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ClusterOperation extends BaseOperation implements ClusterClient {

    public ClusterOperation(){
        super();
    }

    public ClusterOperation(String baseUrl) {
        super(baseUrl);
    }

    public ClusterOperation(String baseUrl, String userName, String token){
        super(baseUrl, userName, token);
    }

    @Override
    public List<ClusterStatus> getClusterStatus(){
        String s = sendGetRequest("/cluster/stats");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseArray(response.getString("data"), ClusterStatus.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public List<ClusterHealth> getHealthStatus() {
        // 实现获取健康状态的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回健康状态信息的字符串表示
        String s = sendGetRequest("/cluster/health");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseArray(response.getString("data"), ClusterHealth.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public PortStatus getPortStatus() {
        // 实现获取端口状态的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回端口状态信息的字符串表示
        String s = sendGetRequest("/servers");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseObject(response.getString("data"), PortStatus.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    @Override
    public String clearLocks() throws IOException {
        // 实现清除锁的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return sendGetRequest("/clean_lock");
    }

    public String getPatitionStatus() throws IOException{
        // 实现获取副本状态的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回端口状态信息的字符串表示
        String s = sendGetRequest("/partitions");
        if(s == null || s.length() == 0){
            return null;
        }
        JSONObject response = JSON.parseObject(s);
        Integer code = response.getInteger("code");
        if(Objects.equals(0,code)){
            return JSON.parseObject(response.getString("data"), String.class);
        }
        throw new VearchException("errorCode:"+code+" errorMsg:"+response.getString("message"));
    }

    /*
        todo 现在没有这个方法，待删除
     */
    @Override
    public String changeMember(int partitionId, int nodeId, int method) throws IOException {
        // 实现副本扩容缩容的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        String endpoint = "/partition/change_member";
        String requestBody = "{\"partition_id\":" + partitionId + ", \"node_id\":" + nodeId + ", \"method\":" + method + "}";
        return sendPostRequest(endpoint, requestBody);
    }
}