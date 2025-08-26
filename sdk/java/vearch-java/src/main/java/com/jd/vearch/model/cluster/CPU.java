package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class CPU {
        @JSONField(name = "total_in_bytes")
        private int totalInBytes;
        @JSONField(name = "user_percent")
        private double userPercent;
        @JSONField(name = "sys_percent")
        private double sysPercent;
        @JSONField(name = "io_wait_percent")
        private double ioWaitPercent;
        @JSONField(name = "idle_percent")
        private double idlePercent;

        public int getTotalInBytes() {
                return totalInBytes;
        }

        public void setTotalInBytes(int totalInBytes) {
                this.totalInBytes = totalInBytes;
        }

        public double getUserPercent() {
                return userPercent;
        }

        public void setUserPercent(double userPercent) {
                this.userPercent = userPercent;
        }

        public double getSysPercent() {
                return sysPercent;
        }

        public void setSysPercent(double sysPercent) {
                this.sysPercent = sysPercent;
        }

        public double getIoWaitPercent() {
                return ioWaitPercent;
        }

        public void setIoWaitPercent(double ioWaitPercent) {
                this.ioWaitPercent = ioWaitPercent;
        }

        public double getIdlePercent() {
                return idlePercent;
        }

        public void setIdlePercent(double idlePercent) {
                this.idlePercent = idlePercent;
        }
}