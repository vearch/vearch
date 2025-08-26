package com.jd.vearch.client;

import com.jd.vearch.model.FieldMatch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author liujishuai
 * @DATE 2025/8/1 8:01 PM
 */
public interface DataClient<T> {

        String insertDocument(String spaceName, Map<String, Object> document, T documentId) throws IOException;

        String bulkInsertDocuments(String spaceName, Map<Long, Map<String, Object>> documents) throws IOException;

        String updateDocument(String spaceName, T documentId, Map<String, Object> document) throws IOException;

        String deleteDocument(String spaceName, T documentId) throws IOException;

        String getDocumentById(String spaceName, T documentId) throws IOException;

        List<Object> bulkGetDocumentsByIds(String spaceName, List<T> documentIds, List<String> fields) throws IOException;

        Object bulkSearchDocumentsByFeatures(String spaceName, List<Map<String, Object>> featureVectors, Map<String, Object> queryParam) throws IOException;

        Object msearchDocumentsByFeatures(String spaceName, Map<String, Object> featureVectors, Map<String, Object> queryParam) throws IOException;

        Object searchDocumentsByIdsAndFeatures(String spaceName, List<T> documentIds, List<String> fields, int size, Map<String, Object> queryParam) throws IOException;

        List<String> multiVectorSearch(String spaceName, List<FieldMatch> fieldMatches, int size) throws IOException;

        Object search(String spaceName, List<Map<String, Object>> sumMap, List<Map<String, Object>> filterMap, Map<String, Object> queryParam) throws IOException;
}