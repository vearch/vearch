package com.jd.vearch.operation;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jd.vearch.client.RagDataClient;
import com.jd.vearch.model.FeatureVectors;
import com.jd.vearch.model.SearchParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RagDataOperation<T> extends BaseOperation implements RagDataClient<T> {

    private String dataBaseName;

    public RagDataOperation(String dataBaseName) {
        super();
        this.dataBaseName = dataBaseName;
    }

    public RagDataOperation(String baseUrl, String dataBaseName) {
        super(baseUrl);
        this.dataBaseName = dataBaseName;
    }

    public RagDataOperation(String baseUrl, String userName, String token, String dataBaseName){
        super(baseUrl, userName, token);
        this.dataBaseName = dataBaseName;
    }

    /*
    id 可有可无
    curl -H "content-type: application/json" -XPOST -d'
    {
        "db_name": "ts_db",
        "space_name": "ts_space",
        "documents": [{
            "_id": "1000000",
            "field_int": 90399,
            "field_float": 90399,
            "field_double": 90399,
            "field_string": "111399",
            "field_vector":  [...]
        }, {
            "_id": "1000001",
            "field_int": 45085,
            "field_float": 45085,
            "field_double": 45085,
            "field_string": "106085",
            "field_vector": [...]
        }, {
            "_id": "1000002",
            "field_int": 52968,
            "field_float": 52968,
            "field_double": 52968,
            "field_string": "113968",
            "field_vector": [...]
        }]
    }
    ' http://${VEARCH_URL}/document/upsert
     */
    @Override
    public String upsertDocuments(String spaceName,List<Map<String, Object>> documents) throws IOException {
        // 实现批量插入或更新的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等

        String endpoint = "/document/upsert";

        HashMap<String, Object> body = new HashMap<>();
        body.put("db_name", dataBaseName);
        body.put("space_name", spaceName);
        body.put("documents", documents);

        return this.sendPostRequest(endpoint, body);
    }

    /*
    curl -H "content-type: application/json" -XPOST -d'
    {
        "db_name": "ts_db",
        "space_name": "ts_space",
        "document_ids": ["4501743250723073467", "616335952940335471", "-2422965400649882823"]
    }
    ' http://${VEARCH_URL}/document/delete
     */
    @Override
    public String deleteDocumentByIds(String spaceName, List<T> documentIds) throws IOException {
        // 实现删除文档的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等

        String endpoint = "/document/delete";

        HashMap<String, Object> body = new HashMap<>();
        body.put("db_name", dataBaseName);
        body.put("space_name", spaceName);
        body.put("document_ids", documentIds);

        return sendPostRequest(endpoint, body);
    }

    /*
    curl -H "content-type: application/json" -XPOST -d'
    {
        "db_name": "ts_db",
        "space_name": "ts_space",
        "filters": {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_int",
                    "operator": ">=",
                    "value": 1
                },
                {
                    "field": "field_int",
                    "operator": "<=",
                    "value": 3
                }
            ]
        },
        "limit": 3
    }
    ' http://${VEARCH_URL}/document/delete
     */
    @Override
    public String deleteDocumentByFilter(String spaceName, Map<String, Object> filters) throws IOException {
        // 实现删除文档的逻辑
        // 发送HTTP请求到相应的端点，处理响应结果等

        String endpoint = "/document/delete";

        HashMap<String, Object> body = new HashMap<>();
        body.put("db_name", dataBaseName);
        body.put("space_name", spaceName);
        body.put("filters", filters);

        return sendPostRequest(endpoint, body);
    }

    /*
    curl -H "content-type: application/json" -XPOST -d'
    {
        "db_name": "ts_db",
        "space_name": "ts_space",
        "document_ids": ["6560995651113580768", "-5621139761924822824", "-104688682735192253"],
        "vector_value": true
    }
    ' http://${VEARCH_URL}/document/query
     */
    @Override
    public List<Object> queryDocumentsByIds(String spaceName, List<T> documentIds) throws IOException {
        // 实现批量根据ID查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示

        String endpoint = "/document/query";

        HashMap<String, Object> body = new HashMap<>();
        body.put("db_name", dataBaseName);
        body.put("space_name", spaceName);
        body.put("document_ids", documentIds);

        String s = sendPostRequest(endpoint, body);

        return JSON.parseArray(s,Object.class);
    }

    /*
    curl -H "content-type: application/json" -XPOST -d'
    {
        "vectors": [
            {
        "field": "field_name",
        "feature": [0.1, 0.2, 0.3, 0.4, 0.5],
        "min_score": 0.9
            }
        ]
        "index_params": {
            "metric_type": "L2"
        },
        "fields": ["field1", "field2"],
        "limit": 3,
        "db_name": "ts_db",
        "space_name": "ts_space"
    }
    ' http://${VEARCH_URL}/document/search
    */
    @Override
    public String searchDocumentByFeatures(String spaceName, FeatureVectors featureVectors, SearchParam searchMap) throws IOException {
        // 实现批量特征查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示

        String endpoint = "/document/search";

        String dbName = dataBaseName;
        searchMap.setDbName(dbName);
        searchMap.setSpaceName(spaceName);
        ArrayList<FeatureVectors> vectors = new ArrayList<>();
        vectors.add(featureVectors);
        searchMap.setVectors(vectors);

        String param = JSON.toJSONString(searchMap);
        String s = sendPostRequest(endpoint, param);

        return JSON.parseObject(s, String.class);
    }

    /*
    curl -H "content-type: application/json" -XPOST -d'
    {
        "vectors": [
            {
                "field": "field_vector",
                "feature": [
                    "..."
                ]
            }
        ],
        "filters": {
            "operator": "AND",
            "conditions": [
                {
                    "field": "field_int",
                    "operator": ">=",
                    "value": 1
                },
                {
                    "field": "field_int",
                    "operator": "<=",
                    "value": 3
                },
                {
                    "field": "field_string",
                    "operator": "IN",
                    "value": [
                        "aaa",
                        "bbb"
                    ]
                }
            ]
        },
        "index_params": {
            "metric_type": "L2"
        },
        "limit": 3,
        "db_name": "ts_db",
        "space_name": "ts_space"
    }
    ' http://${VEARCH_URL}/document/search
     */
    @Override
    public List<Object> searchDocumentsByFilterAndFeatures(String spaceName, Map<String, Object> featureVectors, Map<String, Object> filters, Map<String, Object> searchMap) throws IOException {
        // 实现批量特征查询文档的逻辑
        // 发送HTTP请求到相应的端点，解析响应数据等
        // 返回文档列表的字符串表示

        String endpoint = "/document/search";

        HashMap<String, Object> body = new HashMap<>();
        body.put("db_name", dataBaseName);
        body.put("space_name", spaceName);
        body.put("vectors", featureVectors);

        body.putAll(filters);
        body.putAll(searchMap);

        String s = sendPostRequest(endpoint, body);

        return JSON.parseArray(s,Object.class);
    }
}
