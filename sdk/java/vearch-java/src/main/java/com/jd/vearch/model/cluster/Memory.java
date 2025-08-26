package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Memory {
        @JSONField(name = "total_in_bytes")
        private long totalInBytes;
        @JSONField(name = "free_in_bytes")
        private long freeInBytes;
        @JSONField(name = "used_in_bytes")
        private long usedInBytes;
        @JSONField(name = "used_percent")
        private double usedPercent;

        public long getTotalInBytes() {
                return totalInBytes;
        }

        public void setTotalInBytes(long totalInBytes) {
                this.totalInBytes = totalInBytes;
        }

        public long getFreeInBytes() {
                return freeInBytes;
        }

        public void setFreeInBytes(long freeInBytes) {
                this.freeInBytes = freeInBytes;
        }

        public long getUsedInBytes() {
                return usedInBytes;
        }

        public void setUsedInBytes(long usedInBytes) {
                this.usedInBytes = usedInBytes;
        }

        public double getUsedPercent() {
                return usedPercent;
        }

        public void setUsedPercent(double usedPercent) {
                this.usedPercent = usedPercent;
        }
}