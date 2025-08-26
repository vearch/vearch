package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.jd.vearch.client.DataClient;
import com.jd.vearch.model.FieldMatch;

import java.io.IOException;
import java.util.*;

public class DataOperation<T> extends BaseOperation implements DataClient<T> {
    private final String dataBaseName;

    public DataOperation(String databaseName) {
        super();
        this.dataBaseName = databaseName;
    }

    public DataOperation(String baseUrl,String databaseName) {
       super(baseUrl);
       this.dataBaseName = databaseName;
    }

    @Override
    public String insertDocument(String spaceName, Map<String,Object> document,T documentId) throws IOException {
        // 实现单条插入的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return this.sendPostRequest("/"+dataBaseName+"/"+spaceName+"/"+documentId,document);
    }

    @Override
    public String bulkInsertDocuments(String spaceName, Map<Long,Map<String,Object>> documents) throws IOException {
        // 实现批量插入的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        // 构建字符串
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Map<String, Object>> entry : documents.entrySet()) {
            Long id = entry.getKey();
            Map<String, Object> fields = entry.getValue();

            // 构建index行，并添加换行符\n
            sb.append("{\"index\": {\"_id\": \"").append(id).append("\"}}\n");
            // 构建字段行，并添加换行符\n
            sb.append(JSON.toJSONString(fields)).append("\n");
        }
        System.out.println(sb.toString());
        return this.sendPostRequest("/"+dataBaseName+"/"+spaceName+"/_bulk",sb.toString());
    }

    @Override
    public String updateDocument(String spaceName, T documentId, Map<String,Object> updatedDocument) throws IOException {
        // 实现更新文档的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return sendPostRequest("/"+dataBaseName+"/"+spaceName+"/"+documentId+"/_update",updatedDocument);
    }

    @Override
    public String deleteDocument(String spaceName, Object documentId) throws IOException {
        // 实现删除文档的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等
        return sendDeleteRequest("/"+dataBaseName+"/"+spaceName+"/"+documentId);
    }

    @Override
    public String getDocumentById(String spaceName, Object documentId) throws IOException {
        // 实现根据ID查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档的字符串表示
        return sendGetRequest("/"+dataBaseName+"/"+spaceName+"/"+documentId);
    }

    @Override
    public List<Object> bulkGetDocumentsByIds(String spaceName, List<T> documentIds, List<String> fields) throws IOException {
        // 实现批量根据ID查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示
        Map<String,Object> body = new HashMap();
        body.put("ids",documentIds);
        body.put("fields",fields);
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("query",body);
        String s = sendPostRequest("/" + dataBaseName + "/" + spaceName + "/_query_byids", queryMap);
        return JSON.parseArray(s,Object.class);
    }

    @Override
    public Object bulkSearchDocumentsByFeatures(String spaceName, List<Map<String,Object>> featureVectors,Map<String,Object> queryParam) throws IOException {
        // 实现批量特征查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示
        List<Map<String,Object>> queryMaps = new ArrayList<>();
        for (Map<String, Object> featureVector : featureVectors) {
            Map<String, Object> queryMap = new HashMap<>();
            Map<String, Object> sumMap = new HashMap<>();
            sumMap.put("sum", Arrays.asList(featureVector));
            queryMap.put("query",sumMap);
            Map<String, Object> mergedMap = new HashMap<>(queryMap);
            queryParam.forEach((key, value) -> mergedMap.merge(key, value, (v1, v2) -> v2));
            queryMaps.add(mergedMap);
        }
        String s = sendPostRequest("/" + dataBaseName + "/" + spaceName + "/_bulk_search", JSON.toJSONString(queryMaps));
        return JSON.parse(s);
    }

    @Override
    public Object msearchDocumentsByFeatures(String spaceName,Map<String,Object> featureVector,Map<String,Object> queryParam) throws IOException {
        Map<String, Object> queryMap = new HashMap<>();
        Map<String, Object> sumMap = new HashMap<>();
        sumMap.put("sum", Arrays.asList(featureVector));
        queryMap.put("query",sumMap);
        Map<String, Object> mergedMap = new HashMap<>(queryMap);
        queryParam.forEach((key, value) -> mergedMap.merge(key, value, (v1, v2) -> v2));
        String s = sendPostRequest("/" + dataBaseName + "/" + spaceName + "/_msearch", JSON.toJSONString(mergedMap));
        return JSON.parse(s);
    }

    @Override
    public Object searchDocumentsByIdsAndFeatures(String spaceName, List<T> documentIds, List<String> fields,int size,Map<String,Object> queryParam) throws IOException {
        // 实现根据ID和特征查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示
        Map<String,Object> body = new HashMap();
        body.put("size",size);
        Map<String,Object> query = new HashMap();
        List<Map<String,Object>> sum = new ArrayList<>();
        for (String field : fields) {
            Map<String,Object> filedMap = new HashMap<>();
            filedMap.put("field",field);
            filedMap.put("feature",new ArrayList<>());
            sum.add(filedMap);
        }
        query.put("sum",sum);
        query.put("ids",documentIds);
        body.put("query",query);
        Map<String, Object> mergedMap = new HashMap<>(body);
        queryParam.forEach((key, value) -> mergedMap.merge(key, value, (v1, v2) -> v2));
        String s = sendPostRequest("/" + dataBaseName + "/" + spaceName + "/_query_byids_feature", mergedMap);
        return JSON.parse(s);
    }

    @Override
    public List<String> multiVectorSearch(String spaceName, List<FieldMatch> fieldMatches, int size) throws IOException {
        // 实现多向量查询的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示
        Map<String,Object> body = new HashMap();
        body.put("size",size);
        Map<String,Object> query = new HashMap();
        query.put("sum",fieldMatches);

        body.put("query",query);
        sendPostRequest("/"+dataBaseName+"/"+spaceName+"/_query_byids_feature",body);
        return null;
    }

    @Override
    public Object search(String spaceName, List<Map<String, Object>> sumMap, List<Map<String, Object>> filterMap, Map<String, Object> queryParam) throws IOException {
        Map<String, Object> queryMap = new HashMap<>();
        if(!sumMap.isEmpty()){
            List<Map<String, Object>> sumList = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : sumMap) {
                sumList.add(stringObjectMap);
            }
            queryMap.put("sum",sumList);
        }
        if(!filterMap.isEmpty()){
            List<Map<String, Object>> filterList = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : filterMap) {
                filterList.add(stringObjectMap);
            }
            queryMap.put("filter",filterList);
        }
        Map<String, Object> boby = new HashMap<>();
        boby.put("query",queryMap);
        Map<String, Object> mergedMap = new HashMap<>(boby);
        queryParam.forEach((key, value) -> mergedMap.merge(key, value, (v1, v2) -> v2));
        String s = sendPostRequest("/" + dataBaseName + "/" + spaceName + "/_search", mergedMap);
        return JSON.parse(s);
    }
}