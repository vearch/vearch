package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

public class SearchIndexParam {

    @JSONField(name = "metric_type")
    private String metricType = "L2";
    private int efSearch = 64;
    @JSONField(name = "parallel_on_queries")
    private int parallelOnQueries = 0;
    @JSONField(name = "recall_num")
    private int recallNum = 3;
    private int nprobe = 20;

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public int getEfSearch() {
        return efSearch;
    }

    public void setEfSearch(int efSearch) {
        this.efSearch = efSearch;
    }

    public int getParallelOnQueries() {
        return parallelOnQueries;
    }

    public void setParallelOnQueries(int parallelOnQueries) {
        this.parallelOnQueries = parallelOnQueries;
    }

    public int getRecallNum() {
        return recallNum;
    }

    public void setRecallNum(int recallNum) {
        this.recallNum = recallNum;
    }

    public int getNprobe() {
        return nprobe;
    }

    public void setNprobe(int nprobe) {
        this.nprobe = nprobe;
    }
}
