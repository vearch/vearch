package com.jd.vearch.model.cluster;

import java.util.List;

public class PortStatus {
    public List<Server> servers;
    public int count;

    public List<Server> getServers() {
        return servers;
    }

    public void setServers(List<Server> servers) {
        this.servers = servers;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}