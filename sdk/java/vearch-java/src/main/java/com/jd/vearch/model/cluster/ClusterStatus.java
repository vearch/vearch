package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class ClusterStatus {
    private int status;
    private String ip;
    private List<Label> labels;
    private Memory mem;
    private Swap swap;
    private FileSystem fs;
    private CPU cpu;
    private Network net;
    private GC gc;
    @JSONField(name = "active_conn")
    private int activeConn;

    @JSONField(name = "partition_infos")
    private List<Partition> partitions;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public void setLabels(List<Label> labels) {
        this.labels = labels;
    }

    public Memory getMem() {
        return mem;
    }

    public void setMem(Memory mem) {
        this.mem = mem;
    }

    public Swap getSwap() {
        return swap;
    }

    public void setSwap(Swap swap) {
        this.swap = swap;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public CPU getCpu() {
        return cpu;
    }

    public void setCpu(CPU cpu) {
        this.cpu = cpu;
    }

    public Network getNet() {
        return net;
    }

    public void setNet(Network net) {
        this.net = net;
    }

    public GC getGc() {
        return gc;
    }

    public void setGc(GC gc) {
        this.gc = gc;
    }

    public int getActiveConn() {
        return activeConn;
    }

    public void setActiveConn(int activeConn) {
        this.activeConn = activeConn;
    }

    public List<Partition> getPartitionInfos() {
        return partitions;
    }

    public void setPartitionInfos(List<Partition> partitions) {
        this.partitions = partitions;
    }
}