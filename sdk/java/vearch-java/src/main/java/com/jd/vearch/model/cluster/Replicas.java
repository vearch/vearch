package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Replicas {
    @JSONField(name = "1")
    private Replica replica1;

    @JSONField(name = "2")
    private Replica replica2;
    @JSONField(name = "3")
    private Replica replica3;
    @JSONField(name = "4")
    private Replica replica4;
    @JSONField(name = "5")
    private Replica replica5;
    @JSONField(name = "6")
    private Replica replica6;

    public Replica getReplica1() {
        return replica1;
    }

    public void setReplica1(Replica replica1) {
        this.replica1 = replica1;
    }

    public Replica getReplica2() {
        return replica2;
    }

    public void setReplica2(Replica replica2) {
        this.replica2 = replica2;
    }

    public Replica getReplica3() {
        return replica3;
    }

    public void setReplica3(Replica replica3) {
        this.replica3 = replica3;
    }

    public Replica getReplica4() {
        return replica4;
    }

    public void setReplica4(Replica replica4) {
        this.replica4 = replica4;
    }

    public Replica getReplica5() {
        return replica5;
    }

    public void setReplica5(Replica replica5) {
        this.replica5 = replica5;
    }

    public Replica getReplica6() {
        return replica6;
    }

    public void setReplica6(Replica replica6) {
        this.replica6 = replica6;
    }
}
