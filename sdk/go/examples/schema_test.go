package examples

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/vearch/vearch/v3/sdk/go"
	"github.com/vearch/vearch/v3/sdk/go/auth"
	"github.com/vearch/vearch/v3/sdk/go/entities/models"
)

func setupClient(t *testing.T) *client.Client {
	host := "http://127.0.0.1:9001"
	user := "root"
	secret := "secret"

	authConfig := auth.BasicAuth{UserName: user, Secret: secret}
	c, err := client.NewClient(client.Config{Host: host, AuthConfig: authConfig})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSchemaCreateDB(t *testing.T) {
	ctx := context.Background()

	dbName := "ts_db"
	spaceName := "ts_space"
	c := setupClient(t)
	var db *models.DB
	var space *models.Space

	t.Run("Test CreateDatabase", func(t *testing.T) {
		db = &models.DB{
			Name: dbName,
		}
		err := c.Schema().DBCreator().WithDB(db).Do(ctx)
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
					Name: "field_string",
					Type: "string",
					Index: &models.Index{
						Name: "string",
						Type: "SCALAR",
					},
				},
				{
					Name: "field_float",
					Type: "float",
					Index: &models.Index{
						Name: "float",
						Type: "SCALAR",
					},
				},
				{
					Name: "field_int",
					Type: "integer",
					Index: &models.Index{
						Name: "integer",
						Type: "SCALAR",
					},
				},
				{
					Name: "field_double",
					Type: "double",
					Index: &models.Index{
						Name: "double",
						Type: "SCALAR",
					},
				},
				{
					Name:      "field_vector",
					Type:      "vector",
					Dimension: 128,
					StoreType: "MemoryOnly",
					Index: &models.Index{
						Name: "gamma",
						Type: "HNSW",
						Params: &models.IndexParams{
							MetricType:        "InnerProduct",
							TrainingThreshold: 0,
							EfConstruction:    64,
							EfSearch:          32,
						},
					},
				},
			},
		}

		err := spaceCreator.WithDBName(dbName).WithSpace(space).Do(ctx)
		require.Nil(t, err)
	})
}

func TestSchemaDeleteDB(t *testing.T) {
	ctx := context.Background()
	dbName := "ts_db"
	spaceName := "ts_space"
	c := setupClient(t)

	t.Run("Test DeleteSpace", func(t *testing.T) {
		err := c.Schema().SpaceDeleter().WithDBName(dbName).WithSpaceName(spaceName).Do(ctx)
		require.Nil(t, err)
	})

	t.Run("Test DeleteDatabase", func(t *testing.T) {
		err := c.Schema().DBDeleter().WithDBName(dbName).Do(ctx)
		require.Nil(t, err)
	})
}
