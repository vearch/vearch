package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import java.util.List;

public class ClusterHealth {
    @JSONField(name = "db_name")
    private String dbName;
    @JSONField(name = "space_num")
    private int spaceNum;
    @JSONField(name = "doc_num")
    private int docNum;
    private long size;
    private List<Space> spaces;
    private String status;
    private Object errors;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getSpaceNum() {
        return spaceNum;
    }

    public void setSpaceNum(int spaceNum) {
        this.spaceNum = spaceNum;
    }

    public int getDocNum() {
        return docNum;
    }

    public void setDocNum(int docNum) {
        this.docNum = docNum;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public List<Space> getSpaces() {
        return spaces;
    }

    public void setSpaces(List<Space> spaces) {
        this.spaces = spaces;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getErrors() {
        return errors;
    }

    public void setErrors(Object errors) {
        this.errors = errors;
    }

    // getters and setters




}