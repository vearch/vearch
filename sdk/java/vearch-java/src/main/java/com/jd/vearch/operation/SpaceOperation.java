package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.jd.vearch.client.SpaceClient;
import com.jd.vearch.model.Cache;
import com.jd.vearch.model.Space;

import java.io.IOException;
import java.util.List;

/**
 * @Author liujishuai
 * @DATE 2025/8/1 8:34 PM
 */
public class SpaceOperation extends  BaseOperation implements SpaceClient {

    private final String databaseName;

    public SpaceOperation(String databaseName) {
        super();
        this.databaseName = databaseName;
    }

    public SpaceOperation(String baseUrl, String databaseName) {
        super(baseUrl);
        this.databaseName = databaseName;
    }

    public SpaceOperation(String baseUrl, String userName, String token, String databaseName){
        super(baseUrl, userName, token);
        this.databaseName = databaseName;
    }

    @Override
    public String createSpace(Space space) {
        // 实现创建表空间的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        String endpoint="/dbs/"+databaseName+"/spaces";

        return sendPostRequest(endpoint, JSON.toJSONString(space));
    }

    @Override
    public String viewSpace(String spaceName) {
        // 实现查看表空间的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回表空间信息的字符串表示
        return sendGetRequest("/dbs/"+databaseName+"/spaces/"+ spaceName);
    }

    @Override
    public String deleteSpace(String spaceName) {
        // 实现删除表空间的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return sendDeleteRequest("/dbs/"+databaseName+"/spaces/"+ spaceName);
    }

    /*
        todo 待删除
     */
    @Override
    public String modifyCacheSize(String spaceName,List<Cache> caches) {
        // 实现修改cache大小的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return sendPostRequest("/space/"+databaseName+"/"+ spaceName,JSON.toJSONString(caches));
    }

    /*
        todo 待删除
     */
    @Override
    public String viewCacheSize(String spaceName) {
        // 实现查看cache大小的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回cache大小的整数值
        return sendGetRequest("/config/"+databaseName+"/"+ spaceName);
    }
}
