package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class FeatureVectors {

    /**
     * 向量对应字段名
     */
    private String field;

    /**
     * 检索向量
     */
    private List<Float> feature;

    /**
     * 检索相似度阈值
     */
    @JSONField(name = "min_score")
    private double minScore;

    public void setField(String field){
        this.field = field;
    }

    public String getField(){
        return this.field;
    }

    public void setFeature(List<Float> feature){
        this.feature = feature;
    }

    public List<Float> getFeature(){
        return this.feature;
    }

    public void setMinScore(double minScore){
        this.minScore = minScore;
    }

    public double getMinScore(){
        return this.minScore;
    }

}

