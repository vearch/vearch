package com.demo.langchain4j;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.vearch.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.singletonList;

public class VearchExample {

    static VearchContainer vearch = new VearchContainer();

    static EmbeddingStore<TextSegment> embeddingStore;

    static String databaseName = "embedding_db";

    static String spaceName = "embedding_space_" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);

    private static void buildEmbeddingStoreWithMetadata() {
        buildEmbeddingStore(true);
    }

    private static void buildEmbeddingStoreWithoutMetadata() {
        buildEmbeddingStore(false);
    }

    private static void buildEmbeddingStore(boolean withMetadata) {
        String embeddingFieldName = "text_embedding";
        String textFieldName = "text";
        // Your metadata here
        Map<String, Object> metadata = new HashMap<>();

        // init properties
        Map<String, SpacePropertyParam> properties = new HashMap<>(4);
        properties.put(embeddingFieldName, SpacePropertyParam.VectorParam.builder()
                .index(true)
                .storeType(SpaceStoreType.MEMORY_ONLY)
                .dimension(384)
                .build());
        properties.put(textFieldName, SpacePropertyParam.StringParam.builder().build());
        if (withMetadata) {
            // metadata
            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String || value instanceof UUID) {
                    properties.put(key, SpacePropertyParam.StringParam.builder().build());
                } else if (value instanceof Integer) {
                    properties.put(key, SpacePropertyParam.IntegerParam.builder().build());
                } else if (value instanceof Float) {
                    properties.put(key, SpacePropertyParam.FloatParam.builder().build());
                } else {
                    properties.put(key, SpacePropertyParam.StringParam.builder().build());
                }
            }
        }

        // init vearch config
        VearchConfig vearchConfig = VearchConfig.builder()
                .spaceEngine(SpaceEngine.builder()
                        .name("gamma")
                        .indexSize(1L)
                        .retrievalType(RetrievalType.FLAT)
                        .retrievalParam(RetrievalParam.FLAT.builder()
                                .build())
                        .build())
                .properties(properties)
                .embeddingFieldName(embeddingFieldName)
                .textFieldName(textFieldName)
                .databaseName(databaseName)
                .spaceName(spaceName)
                .modelParams(singletonList(ModelParam.builder()
                        .modelId("vgg16")
                        .fields(singletonList("string"))
                        .out("feature")
                        .build()))
                .build();
        if (withMetadata) {
            vearchConfig.setMetadataFieldNames(new ArrayList<>(metadata.keySet()));
        }

        // init embedding store
        String baseUrl = "http://" + vearch.getHost() + ":" + vearch.getMappedPort(9001);
        embeddingStore = VearchEmbeddingStore.builder()
                .vearchConfig(vearchConfig)
                .baseUrl(baseUrl)
                .build();
    }

    public static void main(String[] args) {
        vearch.start();

        buildEmbeddingStore(false);

        EmbeddingModel embeddingModel = new AllMiniLmL6V2EmbeddingModel();

        TextSegment segment1 = TextSegment.from("I like football.");
        Embedding embedding1 = embeddingModel.embed(segment1).content();
        embeddingStore.add(embedding1, segment1);

        TextSegment segment2 = TextSegment.from("The weather is good today.");
        Embedding embedding2 = embeddingModel.embed(segment2).content();
        embeddingStore.add(embedding2, segment2);

        Embedding queryEmbedding = embeddingModel.embed("What is your favourite sport?").content();
        List<EmbeddingMatch<TextSegment>> relevant = embeddingStore.findRelevant(queryEmbedding, 1);
        EmbeddingMatch<TextSegment> embeddingMatch = relevant.get(0);

        System.out.println(embeddingMatch.score()); // 0.8144288659095
        System.out.println(embeddingMatch.embedded().text()); // I like football.

        vearch.stop();
    }
}
