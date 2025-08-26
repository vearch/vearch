package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

import javax.print.DocFlavor;

public class Engine {

    private String name;
    @JSONField(name = "index_size")
    private int indexSize;
    @JSONField(name = "id_type")
    private String idType;
    @JSONField(name = "metric_type")
    private String metricType;
    @JSONField(name = "retrieval_type")
    private String retrievalType;
    @JSONField(name = "retrieval_param")
    private RetrievalParam retrievalParam;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndexSize() {
        return indexSize;
    }

    public void setIndexSize(int indexSize) {
        this.indexSize = indexSize;
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public String getRetrievalType() {
        return retrievalType;
    }

    public void setRetrievalType(String retrievalType) {
        this.retrievalType = retrievalType;
    }

    public RetrievalParam getRetrievalParam() {
        return retrievalParam;
    }

    public void setRetrievalParam(RetrievalParam retrievalParam) {
        this.retrievalParam = retrievalParam;
    }
}