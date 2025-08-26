package com.jd.vearch.model.cluster;

import java.util.List;

/**
 * @Author liujishuai
 * @DATE 2025/8/2 4:30 PM
 */
public class Server {
    private List<ServerInfo> servers;
    private List<Partition> partitions;

    public List<ServerInfo> getServers() {
        return servers;
    }

    public void setServers(List<ServerInfo> servers) {
        this.servers = servers;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }
}
