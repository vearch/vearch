package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;
import java.util.Map;

public class Space{
    @JSONField(name = "name")
    private String name;

    @JSONField(name = "partition_num")
    private int partitionNum;

    @JSONField(name = "replica_num")
    private int replicaNum;

    @JSONField(name = "fields")
    private List<Field> fields;

    public void setName(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public void setPartitionNum(int partitionNum){
        this.partitionNum = partitionNum;
    }

    public int getPartitionNum() {
        return this.partitionNum;
    }

    public void setReplicaNum(int replicaNum){
        this.partitionNum = replicaNum;
    }

    public int getReplicaNum(){
        return this.replicaNum;
    }

    public void setFields(List<Field> fields){
        this.fields = fields;
    }

    public List<Field> getFields(){
        return this.fields;
    }
}