package com.jd.vearch;

import com.jd.vearch.client.*;
import com.jd.vearch.operation.ClusterOperation;
import com.jd.vearch.operation.DataOperation;
import com.jd.vearch.operation.DatabaseOperation;
import com.jd.vearch.operation.SpaceOperation;

public class VearchClient {
    private final String baseUrl;

    public VearchClient(String baseUrl) {
        this.baseUrl = baseUrl;

    }

    public DatabaseClient getDatabaseClient() {
        return new DatabaseOperation(baseUrl);
    }

    public ClusterClient getClusterClient() {
        return new ClusterOperation(baseUrl);
    }

    public SpaceClient getSpaceClient(String databaseName) {
        return new SpaceOperation(baseUrl, databaseName);
    }

    public DataClient getDataClient(String databaseName) {
        return new DataOperation(baseUrl,databaseName);
    }

}