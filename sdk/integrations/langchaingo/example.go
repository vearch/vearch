package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/vearch/vearch/v3/sdk/integrations/langchaingo/vearch"
	// "github.com/vearch/vearch/v3/internal/entity"
	// "github.com/zhanghexian/langchaingo/vectorstores/vearch"
	// "github.com/tmc/langchaingo/vectorstores/vearch"
)

func main() {
	// Create an embeddings client using the OpenAI API. Requires environment variable OPENAI_API_KEY to be set.
	// fmt.Println("now begining")

	llm, err := openai.New(openai.WithEmbeddingModel("model/bge-small-en-v1.5")) // Specify your preferred embedding model
	if err != nil {
		log.Fatal(err)
	}

	e, err := embeddings.NewEmbedder(llm)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Create a new Pinecone vector store.
	store, err := vearch.New(
		vearch.WithDbName("go_test"),
		vearch.WithNameSpace("langchaingo1"),
		vearch.WithURL("http://liama-index-router.vectorbase.svc.sq01.n.jd.local"),
		vearch.WithEmbedder(e),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Add documents to the Pinecone vector store.
	_, err = store.AddDocuments(context.Background(), []schema.Document{
		{
			PageContent: "Tokyo",
			Metadata: map[string]any{
				"population": 38,
				"area":       2190,
			},
		},
		{
			PageContent: "Paris",
			Metadata: map[string]any{
				"population": 11,
				"area":       105,
			},
		},
		{
			PageContent: "London",
			Metadata: map[string]any{
				"population": 9.5,
				"area":       1572,
			},
		},
		{
			PageContent: "Santiago",
			Metadata: map[string]any{
				"population": 6.9,
				"area":       641,
			},
		},
		{
			PageContent: "Buenos Aires",
			Metadata: map[string]any{
				"population": 15.5,
				"area":       203,
			},
		},
		{
			PageContent: "Rio de Janeiro",
			Metadata: map[string]any{
				"population": 13.7,
				"area":       1200,
			},
		},
		{
			PageContent: "Sao Paulo",
			Metadata: map[string]any{
				"population": 22.6,
				"area":       1523,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Search for similar documents.
	docs, err := store.SimilaritySearch(ctx, "japan", 1)
	fmt.Println(docs)

	// Search for similar documents using score threshold.
	docs, err = store.SimilaritySearch(ctx, "only cities in south america", 10, vectorstores.WithScoreThreshold(0.80))
	fmt.Println(docs)

	// Search for similar documents using score threshold and metadata filter.
	filter := map[string]interface{}{
		"$and": []map[string]interface{}{
			{
				"area": map[string]interface{}{
					"$gte": 1000,
				},
			},
			{
				"population": map[string]interface{}{
					"$gte": 15.5,
				},
			},
		},
	}

	docs, err = store.SimilaritySearch(ctx, "only cities in south america",
		10,
		vectorstores.WithScoreThreshold(0.80),
		vectorstores.WithFilters(filter))
	fmt.Println(docs)
}
