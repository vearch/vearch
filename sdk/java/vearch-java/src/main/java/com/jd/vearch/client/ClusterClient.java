package com.jd.vearch.client;

import com.jd.vearch.model.cluster.ClusterHealth;
import com.jd.vearch.model.cluster.ClusterStatus;
import com.jd.vearch.model.cluster.PortStatus;

import java.io.IOException;
import java.util.List;

public interface ClusterClient {
    /**
     * 获取集群状态
     * @return
     * @throws IOException
     */
    List<ClusterStatus> getClusterStatus() ;

    /**
     * 获取健康状态
     * @return
     * @throws IOException
     */
    List<ClusterHealth> getHealthStatus() ;

    /**
     * 获取端口状态
     * @return
     * @throws IOException
     */
    PortStatus getPortStatus();
    /**
     * 清除锁
     * @throws IOException
     */
    String clearLocks() throws IOException;
    /**
     * 副本扩容缩容
     * @param partitionId
     * @param nodeId
     * @param method
     * @throws IOException
     */
    String changeMember(int partitionId, int nodeId, int method) throws IOException;
}
