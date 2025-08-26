package com.jd.vearch.model;


import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.List;

/**
 * HashMap<String, Object> featureVectors = new HashMap<>();
 *         featureVectors.put("field", "embedding");
 *         featureVectors.put("feature", embeddingList);
 *         featureVectors.put("min_score", 0.8);
 *
 *
 *         // 3. 构建检索参数
 *         HashMap<String, Object> searchMap = new HashMap<>();
 *
 *         // 向量索引参数
 *         HashMap<String, Object> indexMap = new HashMap<>();
 *         indexMap.put("metric_type", "L2");
 *         indexMap.put("efSearch", 64);
 *         indexMap.put("parallel_on_queries", 0);
 *         indexMap.put("recall_num", 3);
 *         indexMap.put("nprobe", 20);
 *         searchMap.put("index_params", indexMap);
 *
 *         // 返回域参数
 *         ArrayList<String> callBackFields = new ArrayList<>();
 *         callBackFields.add("documentId");
 *         callBackFields.add("content");
 *         callBackFields.add("metadata");
 *         searchMap.put("fields", callBackFields);
 *
 *         // 返回结果个数
 *         searchMap.put("limit", 3);
 *
 *         searchMap.put("is_brute_search", 1);
 *
 *         body.put("db_name", dataBaseName);
 *         body.put("space_name", spaceName);
 *         ArrayList<Map<String, Object>> vectors = new ArrayList<>();
 *         vectors.add(featureVectors);
 *         body.put("vectors", vectors);
 */


public class SearchParam {

    /**
     * 数据库名
     */
    @JSONField(name = "db_name")
    private String dbName;

    /**
     * 表名
     */
    @JSONField(name = "space_name")
    private String spaceName;

    /**
     * 查询向量列表
     */
    private ArrayList<FeatureVectors> vectors;

    /**
     * 返回字段
     */
    private List<String> fields;

    /**
     * 返回个数
     */
    private int limit = 3;

    /**
     * 全量搜索开关 1 走全量搜索
     */
    @JSONField(name = "is_brute_search")
    private int isBruteSearch = 0;

    /**
     * 模糊检索向量参数
     */
    @JSONField(name = "index_params")
    private SearchIndexParam searchIndexParam;

    public void setDbName(String dbName){
        this.dbName = dbName;
    }

    public String getDbName(){
        return this.dbName;
    }

    public void setSpaceName(String spaceName){
        this.spaceName = spaceName;
    }

    public String getSpaceName(){
        return this.spaceName;
    }

    public void setVectors(ArrayList<FeatureVectors> vectors){
        this.vectors = vectors;
    }

    public ArrayList<FeatureVectors> getVectors(){
        return this.vectors;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getIsBruteSearch() {
        return isBruteSearch;
    }

    public void setIsBruteSearch(int isBruteSearch) {
        this.isBruteSearch = isBruteSearch;
    }

    public SearchIndexParam getSearchIndexParam() {
        return searchIndexParam;
    }

    public void setSearchIndexParam(SearchIndexParam searchIndexParam) {
        this.searchIndexParam = searchIndexParam;
    }
}
