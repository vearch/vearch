package com.demo.langchain4j;

import dev.langchain4j.community.store.embedding.vearch.MetricType;
import dev.langchain4j.community.store.embedding.vearch.VearchConfig;
import dev.langchain4j.community.store.embedding.vearch.VearchEmbeddingStore;
import dev.langchain4j.community.store.embedding.vearch.field.*;
import dev.langchain4j.community.store.embedding.vearch.index.HNSWParam;
import dev.langchain4j.community.store.embedding.vearch.index.Index;
import dev.langchain4j.community.store.embedding.vearch.index.IndexType;
import dev.langchain4j.community.store.embedding.vearch.index.search.HNSWSearchParam;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class VearchExample {

    private static final Logger log = LoggerFactory.getLogger(VearchExample.class);

    static VearchContainer vearch = new VearchContainer();
    static EmbeddingStore<TextSegment> embeddingStore;
    static String databaseName = "embedding_db";
    static String spaceName = "embedding_space_" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
    static EmbeddingModel embeddingModel = new AllMiniLmL6V2EmbeddingModel();

    private static void buildEmbeddingStore(boolean withMetadata) {
        String embeddingFieldName = "text_embedding";
        String textFieldName = "text";
        // Your metadata here
        Map<String, Object> metadata = new HashMap<>();

        // init fields
        List<Field> fields = new ArrayList<>(4);
        List<String> metadataFieldNames = new ArrayList<>();
        fields.add(VectorField.builder()
                .name(embeddingFieldName)
                .dimension(embeddingModel.dimension())
                .index(Index.builder()
                        .name("gamma")
                        .type(IndexType.HNSW)
                        .params(HNSWParam.builder()
                                .metricType(MetricType.INNER_PRODUCT)
                                .efConstruction(100)
                                .nLinks(32)
                                .efSearch(64)
                                .build())
                        .build())
                .build());
        fields.add(StringField.builder()
                .name(textFieldName)
                .fieldType(FieldType.STRING)
                .build());
        if (withMetadata) {
            // metadata
            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String || value instanceof UUID) {
                    fields.add(StringField.builder()
                            .name(key)
                            .fieldType(FieldType.STRING)
                            .build());
                } else if (value instanceof Integer) {
                    fields.add(NumericField.builder()
                            .name(key)
                            .fieldType(FieldType.INTEGER)
                            .build());
                } else if (value instanceof Long) {
                    fields.add(NumericField.builder()
                            .name(key)
                            .fieldType(FieldType.LONG)
                            .build());
                } else if (value instanceof Float) {
                    fields.add(NumericField.builder()
                            .name(key)
                            .fieldType(FieldType.FLOAT)
                            .build());
                } else if (value instanceof Double) {
                    fields.add(NumericField.builder()
                            .name(key)
                            .fieldType(FieldType.DOUBLE)
                            .build());
                }
            }
        }

        // init vearch config
        spaceName = "embedding_space_" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        VearchConfig vearchConfig = VearchConfig.builder()
                .databaseName(databaseName)
                .spaceName(spaceName)
                .textFieldName(textFieldName)
                .embeddingFieldName(embeddingFieldName)
                .fields(fields)
                .metadataFieldNames(metadataFieldNames)
                .searchIndexParam(HNSWSearchParam.builder()
                        .metricType(MetricType.INNER_PRODUCT)
                        .efSearch(64)
                        .build())
                .build();
        if (withMetadata) {
            vearchConfig.setMetadataFieldNames(new ArrayList<>(metadata.keySet()));
        }

        // init embedding store
        String baseUrl = "http://" + vearch.getHost() + ":" + vearch.getMappedPort(9001);
        embeddingStore = VearchEmbeddingStore.builder()
                .vearchConfig(vearchConfig)
                .baseUrl(baseUrl)
                .logRequests(true)
                .logResponses(true)
                .build();
    }

    public static void main(String[] args) {
        vearch.start();

        buildEmbeddingStore(false);

        TextSegment segment1 = TextSegment.from("I like football.");
        Embedding embedding1 = embeddingModel.embed(segment1).content();
        embeddingStore.add(embedding1, segment1);

        TextSegment segment2 = TextSegment.from("The weather is good today.");
        Embedding embedding2 = embeddingModel.embed(segment2).content();
        embeddingStore.add(embedding2, segment2);

        Embedding queryEmbedding = embeddingModel.embed("What is your favourite sport?").content();

        // Wait for finishing adding embedding content.
        awaitUntilAsserted(() -> Assertions.assertThat(getAllEmbeddings()).hasSize(2));

        EmbeddingSearchResult<TextSegment> searchResult = embeddingStore.search(
                EmbeddingSearchRequest.builder()
                        .queryEmbedding(queryEmbedding)
                        .maxResults(1)
                        .build()
        );
        EmbeddingMatch<TextSegment> embeddingMatch = searchResult.matches().get(0);

        log.info("score={}", embeddingMatch.score()); // 0.8144288659095
        log.info("text={}", embeddingMatch.embedded().text()); // I like football.

        vearch.stop();
    }

    static List<EmbeddingMatch<TextSegment>> getAllEmbeddings() {
        EmbeddingSearchRequest embeddingSearchRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(embeddingModel.embed("test").content())
                .maxResults(1000)
                .build();
        EmbeddingSearchResult<TextSegment> searchResult = embeddingStore.search(embeddingSearchRequest);
        return searchResult.matches();
    }

    static void awaitUntilAsserted(ThrowingRunnable assertion) {
        Awaitility.await().atMost(Duration.ofSeconds(60L)).pollDelay(Duration.ofSeconds(0L)).pollInterval(Duration.ofMillis(300L)).untilAsserted(assertion);
    }
}
