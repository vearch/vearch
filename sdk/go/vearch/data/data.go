package data

import "github.com/vearch/vearch/v3/sdk/go/vearch/connection"

type API struct {
	connection *connection.Connection
}

func New(con *connection.Connection) *API {
	return &API{connection: con}
}

func (data *API) Creator() *Creator {
	return &Creator{
		connection: data.connection,
	}
}

func (data *API) Searcher() *Searcher {
	return &Searcher{
		connection: data.connection,
	}
}

func (data *API) Query() *Query {
	return &Query{
		connection: data.connection,
	}
}
