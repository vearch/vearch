package main

import (
	"context"
	"fmt"
	"log"
	"net/url"

	// "github.com/tmc/langchaingo/embeddings"
	embedHug "github.com/tmc/langchaingo/embeddings/huggingface"

	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	// "github.com/vearch/vearch/sdk/integrations/langchaingo/vearch"
	// "github.com/vearch/vearch/sdk/go/vearch/entities/models"
)

func main() {
	// Create an embeddings client using the OpenAI API. Requires environment variable OPENAI_API_KEY to be set.
	e, err := embedHug.NewHuggingface(embedHug.WithModel("text2vec/text2vec-large-chinese"))
	if err != nil {
		log.Fatal(err)

	}
	if err != nil {
		log.Fatal(err)
	}
	url, err := url.Parse("http://liama-index-router.vectorbase.svc.sq01.n.jd.local")
	if err != nil {
		log.Fatal(err)
	}
	// Create a new Vearch vector store.
	store, err := vearch.New(
		vearch.WithDBName("langchaingo_dbt"),
		vearch.WithSpaceName("langchaingo_t"),
		vearch.WithURL(*url),
		vearch.WithEmbedder(e),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Add documents to the Vearch vector store.
	var ids []string
	ids, err = store.AddDocuments(context.Background(), []schema.Document{
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
				"population": 95,
				"area":       1572,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ids", ids)
	// Search for similar documents.
	var docs []schema.Document
	docs, err = store.SimilaritySearch(context.Background(), "japan", 1)
	fmt.Println("&&&&&&&&", docs)

	filter := map[string]interface{}{
		"AND": []map[string]interface{}{
			{
				"condition": map[string]interface{}{
					"Field":    "population",
					"Operator": ">",
					"Value":    20,
				},
			},
		},
	}
	var docs1 []schema.Document
	docs1, err = store.SimilaritySearch(context.Background(), "only cities in earth",
		20,
		vectorstores.WithFilters(filter))
	fmt.Println("*****", docs1)
}
