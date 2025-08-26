package com.jd.vearch.model;

/**
 * @Author liujishuai
 * @DATE 2025/8/1 10:11 PM
 */
public class FieldMatch {
    private String field;
    private double[] feature;
    private Double minScore;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public double[] getFeature() {
        return feature;
    }

    public void setFeature(double[] feature) {
        this.feature = feature;
    }

    public Double getMinScore() {
        return minScore;
    }

    public void setMinScore(Double minScore) {
        this.minScore = minScore;
    }
}
