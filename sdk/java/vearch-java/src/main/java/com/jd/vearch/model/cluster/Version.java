package com.jd.vearch.model.cluster;

import com.alibaba.fastjson.annotation.JSONField;

public class Version {
    @JSONField(name = "build_version")
    public String buildVersion;
    @JSONField(name = "build_time")
    public String buildTime;
    @JSONField(name = "commit_id")
    public String commitId;

    public String getBuildVersion() {
        return buildVersion;
    }

    public void setBuildVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    public String getBuildTime() {
        return buildTime;
    }

    public void setBuildTime(String buildTime) {
        this.buildTime = buildTime;
    }

    public String getCommitId() {
        return commitId;
    }

    public void setCommitId(String commitId) {
        this.commitId = commitId;
    }
}