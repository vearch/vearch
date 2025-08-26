package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

public class Index {

    @JSONField(name = "name")
    private String name;

    @JSONField(name = "type")
    private String type;

    @JSONField(name = "params")
    private RetrievalParam retrievalParam;

    public void setName(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public void setType(String type){
        this.type = type;
    }

    public String getType(){
        return this.type;
    }

    public void setRetrievalParam(RetrievalParam retrievalParam){
        this.retrievalParam = retrievalParam;
    }

    public RetrievalParam getRetrievalParam(){
        return this.retrievalParam;
    }
}
