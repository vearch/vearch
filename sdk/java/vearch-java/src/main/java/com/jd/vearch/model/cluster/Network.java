package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Network {
        @JSONField(name = "in_pre_second")
        private int inPreSecond;
        @JSONField(name = "out_pre_second")
        private int outPreSecond;
        private int connect;

        public int getInPreSecond() {
                return inPreSecond;
        }

        public void setInPreSecond(int inPreSecond) {
                this.inPreSecond = inPreSecond;
        }

        public int getOutPreSecond() {
                return outPreSecond;
        }

        public void setOutPreSecond(int outPreSecond) {
                this.outPreSecond = outPreSecond;
        }

        public int getConnect() {
                return connect;
        }

        public void setConnect(int connect) {
                this.connect = connect;
        }
}