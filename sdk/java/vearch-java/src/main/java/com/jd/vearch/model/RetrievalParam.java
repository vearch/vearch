package com.jd.vearch.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Map;

public  class RetrievalParam {
        @JSONField(name = "metric_type")
        private String metricType;
        private int ncentroids;
        private int nsubvector;

        private int training_threshold = 39;

        @JSONField(name = "hnsw")
        private Hnsw hnsw = new Hnsw();


        public String getMetricType() {
                return metricType;
        }

        public void setMetricType(String metricType) {
                this.metricType = metricType;
        }

        public int getNcentroids() {
                return ncentroids;
        }

        public void setNcentroids(int ncentroids) {
                this.ncentroids = ncentroids;
        }

        public int getNsubvector() {
                return nsubvector;
        }

        public void setNsubvector(int nsubvector) {
                this.nsubvector = nsubvector;
        }

        public void setTraining_threshold(int training_threshold){
                this.training_threshold = training_threshold;
        }

        public int getTraining_threshold(){
                return this.training_threshold;
        }

        public void setHnsw(Hnsw hnsw){
                this.hnsw = hnsw;
        }

        public Hnsw getHnsw(){
                return this.hnsw;
        }
}