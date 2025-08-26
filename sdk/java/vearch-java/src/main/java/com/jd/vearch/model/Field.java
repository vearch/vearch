package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

public class Field {

    @JSONField(name = "name")
    private String name;

    @JSONField(name = "type")
    private String type;

    @JSONField(name = "dimension")
    private int dimension;

    @JSONField(name = "index")
    private Index index;

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

    public void setDimension(int dimension){
        this.dimension = dimension;
    }

    public int getDimension(){
        return this.dimension;
    }

    public void setIndex(Index index){
        this.index = index;
    }

    public Index getIndex(){
        return this.index;
    }

}
