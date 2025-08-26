package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class ServerInfo {
    public int name;
    @JSONField(name = "rpc_port")
    public int rpcPort;
    @JSONField(name = "raft_heartbeat_port")
    public int raftHeartbeatPort;
    @JSONField(name = "raft_heartbeat_port")
    public int raftReplicatePort;
    public String ip;
    @JSONField(name = "p_ids")
    public List<Integer> pIds;
    @JSONField(name = "private")
    public Boolean isPrivate;
    public Version version;

    public int getName() {
        return name;
    }

    public void setName(int name) {
        this.name = name;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getRaftHeartbeatPort() {
        return raftHeartbeatPort;
    }

    public void setRaftHeartbeatPort(int raftHeartbeatPort) {
        this.raftHeartbeatPort = raftHeartbeatPort;
    }

    public int getRaftReplicatePort() {
        return raftReplicatePort;
    }

    public void setRaftReplicatePort(int raftReplicatePort) {
        this.raftReplicatePort = raftReplicatePort;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<Integer> getpIds() {
        return pIds;
    }

    public void setpIds(List<Integer> pIds) {
        this.pIds = pIds;
    }

    public Boolean getPrivate() {
        return isPrivate;
    }

    public void setPrivate(Boolean aPrivate) {
        isPrivate = aPrivate;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }
}