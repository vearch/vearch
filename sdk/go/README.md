# Vearch Go SDK

This README provides examples on how to use the Vearch Go SDK for interacting with Vearch, a scalable distributed system for embedding-based retrieval. The following examples illustrate how to perform common operations such as creating databases and spaces, inserting, querying, searching, and deleting documents.

## Prerequisites

Before you begin, ensure you have the following:

- Go installed on your machine (version 1.22 or later is recommended).
- Access to a running Vearch server.
- Vearch Go SDK installed in your Go workspace.

## Installation

To install the Vearch Go SDK, run the following command:

```sh
go get -u github.com/vearch/vearch/v3/sdk/go
```

## Usage Examples

### Setup Client

To interact with Vearch, you need to set up a client with the appropriate configuration:

```go
import (
    "github.com/vearch/vearch/v3/sdk/go"
    "github.com/vearch/vearch/v3/sdk/go/auth"
)

func setupClient() (*vearch.Client, error) {
    host := "http://127.0.0.1:9001"
    user := "root"
    secret := "secret"

    authConfig := auth.BasicAuth{UserName: user, Secret: secret}
    return vearch.NewClient(vearch.Config{Host: host, AuthConfig: authConfig})
}
```

### Creating a Database and Space

The following example shows how to create a database and a space within that database:

```go
import (
    "context"
    "github.com/vearch/vearch/v3/sdk/go/entities/models"
)

func createDBAndSpace(client *vearch.Client) error {
    ctx := context.Background()
    dbName := "ts_db"
    spaceName := "ts_space"

    // Create Database
    db := &models.DB{Name: dbName}
    if err := client.Schema().DBCreator().WithDB(db).Do(ctx); err != nil {
        return err
    }

    // Define the space schema
    space := &models.Space{
        Name:         spaceName,
        PartitionNum: 1,
        ReplicaNum:   1,
        // ... define fields and index
    }

    // Create Space
    return client.Schema().SpaceCreator().WithDBName(dbName).WithSpace(space).Do(ctx)
}
```

### Inserting Documents

To insert documents into a space:

```go
func upsertDocs(client *vearch.Client) error {
    ctx := context.Background()
    dbName := "ts_db"
    spaceName := "ts_space"
    documents := []interface{}{ /* ... document data ... */ }

    _, err := client.Data().Creator().WithDBName(dbName).WithSpaceName(spaceName).WithDocs(documents).Do(ctx)
    return err
}
```

### Searching Documents

To search for documents using a vector:

```go
func searchDocs(client *vearch.Client) error {
    ctx := context.Background()
    dbName := "ts_db"
    spaceName := "ts_space"
    vector := []models.Vector{ /* ... vector data ... */ }

    result, err := client.Data().Searcher().WithDBName(dbName).WithSpaceName(spaceName).WithLimit(2).WithVectors(vector).Do(ctx)
    if err != nil {
        return err
    }

    fmt.Printf("search result %v\n", result.Docs.Data.Documents...)
    return nil
}
```

### Deleting Documents

To delete documents by their IDs:

```go
func deleteDocs(client *vearch.Client) error {
    ctx := context.Background()
    dbName := "ts_db"
    spaceName := "ts_space"
    ids := []string{"1", "2"}

    result, err := client.Data().Deleter().WithDBName(dbName).WithSpaceName(spaceName).WithIDs(ids).Do(ctx)
    if err != nil {
        return err
    }

    fmt.Printf("delete result %v\n", result.Docs.Data.DocumentsIDs)
    return nil
}
```

## Running Tests

To run the provided tests, execute the following command in your terminal:

```sh
go test -v ./... # Run this in the directory where your test files are located
```

Make sure your Vearch server is running and accessible at the specified host address before running the tests.

## Conclusion

The provided examples are a starting point for integrating the Vearch Go SDK into your application. For more detailed information, please refer to the official Vearch documentation and the Go SDK's godoc.

Remember to handle errors and edge cases as per your application's requirements. The examples above omit comprehensive error handling for brevity.
