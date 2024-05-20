package data

import "github.com/vearch/vearch/sdk/go/v3/connection"

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

func (data *API) Deleter() *Deleter {
	return &Deleter{
		connection: data.connection,
	}
}
