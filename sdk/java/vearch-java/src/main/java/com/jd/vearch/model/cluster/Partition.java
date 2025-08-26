package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Partition {
        private int pid;
        @JSONField(name = "doc_num")
        private int docNum;
        private long size;
        private String path;
        private int status;
        @JSONField(name = "raft_status")
        private RaftStatus raftStatus;
        @JSONField(name = "index_status")
        private int indexStatus;

        @JSONField(name = "replica_num")
        private int replicaNum;

        private String color;
        private String ip;
        @JSONField(name = "node_id")
        private int nodeId;

        public int getPid() {
                return pid;
        }

        public void setPid(int pid) {
                this.pid = pid;
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

        public String getPath() {
                return path;
        }

        public void setPath(String path) {
                this.path = path;
        }

        public int getStatus() {
                return status;
        }

        public void setStatus(int status) {
                this.status = status;
        }

        public RaftStatus getRaftStatus() {
                return raftStatus;
        }

        public void setRaftStatus(RaftStatus raftStatus) {
                this.raftStatus = raftStatus;
        }

        public int getIndexStatus() {
                return indexStatus;
        }

        public void setIndexStatus(int indexStatus) {
                this.indexStatus = indexStatus;
        }

        public int getReplicaNum() {
                return replicaNum;
        }

        public void setReplicaNum(int replicaNum) {
                this.replicaNum = replicaNum;
        }

        public String getColor() {
                return color;
        }

        public void setColor(String color) {
                this.color = color;
        }

        public String getIp() {
                return ip;
        }

        public void setIp(String ip) {
                this.ip = ip;
        }

        public int getNodeId() {
                return nodeId;
        }

        public void setNodeId(int nodeId) {
                this.nodeId = nodeId;
        }
}