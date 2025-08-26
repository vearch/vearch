package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class RaftStatus {
        @JSONField(name = "ID")
        private int id;
        @JSONField(name = "NodeID")
        private int nodeID;
        @JSONField(name = "Leader")
        private int leader;
        @JSONField(name = "Term")
        private int term;
        @JSONField(name = "Index")
        private int index;
        @JSONField(name = "Commit")
        private int commit;
        @JSONField(name = "Applied")
        private int applied;
        @JSONField(name = "Vote")
        private int vote;
        @JSONField(name = "PendQueue")
        private int pendQueue;
        @JSONField(name = "RecvQueue")
        private int recvQueue;
        @JSONField(name = "AppQueue")
        private int appQueue;
        @JSONField(name = "Stopped")
        private boolean stopped;
        @JSONField(name = "RestoringSnapshot")
        private boolean restoringSnapshot;
        @JSONField(name = "State")
        private String state;
        @JSONField(name = "Replicas")
        private Replicas replicas;

        public int getId() {
                return id;
        }

        public void setId(int id) {
                this.id = id;
        }

        public int getNodeID() {
                return nodeID;
        }

        public void setNodeID(int nodeID) {
                this.nodeID = nodeID;
        }

        public int getLeader() {
                return leader;
        }

        public void setLeader(int leader) {
                this.leader = leader;
        }

        public int getTerm() {
                return term;
        }

        public void setTerm(int term) {
                this.term = term;
        }

        public int getIndex() {
                return index;
        }

        public void setIndex(int index) {
                this.index = index;
        }

        public int getCommit() {
                return commit;
        }

        public void setCommit(int commit) {
                this.commit = commit;
        }

        public int getApplied() {
                return applied;
        }

        public void setApplied(int applied) {
                this.applied = applied;
        }

        public int getVote() {
                return vote;
        }

        public void setVote(int vote) {
                this.vote = vote;
        }

        public int getPendQueue() {
                return pendQueue;
        }

        public void setPendQueue(int pendQueue) {
                this.pendQueue = pendQueue;
        }

        public int getRecvQueue() {
                return recvQueue;
        }

        public void setRecvQueue(int recvQueue) {
                this.recvQueue = recvQueue;
        }

        public int getAppQueue() {
                return appQueue;
        }

        public void setAppQueue(int appQueue) {
                this.appQueue = appQueue;
        }

        public boolean isStopped() {
                return stopped;
        }

        public void setStopped(boolean stopped) {
                this.stopped = stopped;
        }

        public boolean isRestoringSnapshot() {
                return restoringSnapshot;
        }

        public void setRestoringSnapshot(boolean restoringSnapshot) {
                this.restoringSnapshot = restoringSnapshot;
        }

        public String getState() {
                return state;
        }

        public void setState(String state) {
                this.state = state;
        }

        public Replicas getReplicas() {
                return replicas;
        }

        public void setReplicas(Replicas replicas) {
                this.replicas = replicas;
        }
}