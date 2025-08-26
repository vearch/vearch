package com.jd.vearch.model.cluster;

import java.util.List;

public class Space {
        private String status;
        private String name;
        private int partition_num;
        private int replica_num;
        private int doc_num;
        private long size;
        private List<Partition> partitions;

        // getters and setters

        public String getStatus() {
                return status;
        }

        public void setStatus(String status) {
                this.status = status;
        }

        public String getName() {
                return name;
        }

        public void setName(String name) {
                this.name = name;
        }

        public int getPartition_num() {
                return partition_num;
        }

        public void setPartition_num(int partition_num) {
                this.partition_num = partition_num;
        }

        public int getReplica_num() {
                return replica_num;
        }

        public void setReplica_num(int replica_num) {
                this.replica_num = replica_num;
        }

        public int getDoc_num() {
                return doc_num;
        }

        public void setDoc_num(int doc_num) {
                this.doc_num = doc_num;
        }

        public long getSize() {
                return size;
        }

        public void setSize(long size) {
                this.size = size;
        }

        public List<Partition> getPartitions() {
                return partitions;
        }

        public void setPartitions(List<Partition> partitions) {
                this.partitions = partitions;
        }
}
