package examples

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/vearch/vearch/sdk/go/v3/vearch"
	"github.com/vearch/vearch/sdk/go/v3/vearch/entities/models"
)

func TestSchemaCreateDB(t *testing.T) {
	ctx := context.Background()
	host := "http://127.0.0.1:9001"
	dbName := "ts_db"
	spaceName := "ts_space"

	c, err := client.NewClient(client.Config{Host: host})
	if err != nil {
		t.Fatal(err)
	}

	var db *models.DB
	var space *models.Space

	t.Run("Test CreateDatabase", func(t *testing.T) {
		db = &models.DB{
			Name: dbName,
		}
		err = c.Schema().DBCreator().WithDB(db).Do(ctx)
		require.Nil(t, err)
	})

	t.Run("Test CreateSpace", func(t *testing.T) {
		spaceCreator := c.Schema().SpaceCreator()
		space = &models.Space{
			Name:         spaceName,
			PartitionNum: 1,
			ReplicaNum:   1,
			Fields: []*models.Field{
				{
					Name: "int",
					Type: "integer",
				},
				{
					Name:      "vector",
					Type:      "vector",
					Dimension: 512,
					StoreType: "MemoryOnly",
					Index: &models.Index{
						IndexName: "gamma",
						IndexType: "HNSW",
						IndexParams: &models.IndexParams{
							MetricType:        "InnerProduct",
							TrainingThreshold: 100000,
							EfConstruction:    64,
							EfSearch:          32,
						},
					},
				},
			},
		}

		err = spaceCreator.WithDBName(db.Name).WithSpace(space).Do(ctx)
		require.Nil(t, err)
	})
}
