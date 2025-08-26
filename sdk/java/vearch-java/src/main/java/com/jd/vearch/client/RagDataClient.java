package com.jd.vearch.client;

import com.jd.vearch.model.FeatureVectors;
import com.jd.vearch.model.SearchParam;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface RagDataClient<T>  {

    String upsertDocuments(String spaceName, List<Map<String, Object>> documents) throws IOException;

    String deleteDocumentByIds(String spaceName, List<T> documentIds) throws IOException;

    String deleteDocumentByFilter(String spaceName, Map<String, Object> filters) throws IOException;

    List<Object> queryDocumentsByIds(String spaceName, List<T> documentIds) throws IOException;

    String searchDocumentByFeatures(String spaceName, FeatureVectors featureVectors, SearchParam searchMap) throws IOException;

    List<Object> searchDocumentsByFilterAndFeatures(String spaceName, Map<String, Object> featureVectors, Map<String, Object> filters,  Map<String, Object> searchMap) throws IOException;

}
