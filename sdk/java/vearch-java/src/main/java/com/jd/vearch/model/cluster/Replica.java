package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Replica {
        @JSONField(name = "Match")
        private int match;
        @JSONField(name = "Commit")
        private int commit;
        @JSONField(name = "Next")
        private int next;
        @JSONField(name = "State")
        private String state;
        @JSONField(name = "Snapshoting")
        private boolean snapshoting;
        @JSONField(name = "Paused")
        private boolean paused;
        @JSONField(name = "Active")
        private boolean active;
        @JSONField(name = "LastActive")
        private long lastActive;
        @JSONField(name = "Inflight")
        private int inflight;


        // getters and setters

        public int getMatch() {
                return match;
        }

        public void setMatch(int match) {
                this.match = match;
        }

        public int getCommit() {
                return commit;
        }

        public void setCommit(int commit) {
                this.commit = commit;
        }

        public int getNext() {
                return next;
        }

        public void setNext(int next) {
                this.next = next;
        }

        public String getState() {
                return state;
        }

        public void setState(String state) {
                this.state = state;
        }

        public boolean isSnapshoting() {
                return snapshoting;
        }

        public void setSnapshoting(boolean snapshoting) {
                this.snapshoting = snapshoting;
        }

        public boolean isPaused() {
                return paused;
        }

        public void setPaused(boolean paused) {
                this.paused = paused;
        }

        public boolean isActive() {
                return active;
        }

        public void setActive(boolean active) {
                this.active = active;
        }

        public long getLastActive() {
                return lastActive;
        }

        public void setLastActive(long lastActive) {
                this.lastActive = lastActive;
        }

        public int getInflight() {
                return inflight;
        }

        public void setInflight(int inflight) {
                this.inflight = inflight;
        }
}